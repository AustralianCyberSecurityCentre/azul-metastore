"""Submit new binaries to Azul dispatcher."""

import asyncio
import logging
from typing import AsyncIterable

from azul_bedrock import models_network as azm
from azul_bedrock.exceptions import ApiException
from azul_bedrock.models_restapi import binaries_data as bedr_bdata
from azul_security.exceptions import SecurityAccessException, SecurityParseException
from starlette.datastructures import UploadFile
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_404_NOT_FOUND,
    HTTP_422_UNPROCESSABLE_ENTITY,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from azul_metastore import context, settings
from azul_metastore.common import data_common, fileformat
from azul_metastore.models import basic_events
from azul_metastore.query import binary_create
from azul_metastore.query.binary2 import (
    binary_read,
    binary_submit_dataless,
    binary_submit_manual,
)

logger = logging.getLogger(__name__)

# Max time to wait for a binary submission to dispatcher (10minutes to allow very large file upload 4GiB etc)
SUBMIT_BINARY_TIMEOUT_SECONDS = 600


def _transform_metadata_to_binary_entity(
    bin_info: azm.Datastream, filename: str, augstreams: list[azm.Datastream]
) -> azm.BinaryEvent.Entity:
    """Transform metadata into an entity."""
    ret = bin_info.to_input_entity()
    if filename:
        ret.features.append(azm.FeatureValue(name="filename", type="filepath", value=filename))
    ret.datastreams += augstreams
    return ret


async def _submit_binary_data_with_sources(
    ctx: context.Context, sources: list[str], label: azm.DataLabel, data: UploadFile | AsyncIterable[bytes]
) -> azm.Datastream:
    """Submit binary data to multiple sources and return the file info."""
    # If there are multiple sources, copy from one source to another after initial submission
    if len(sources) == 0:
        raise Exception("Attempting to upload a binary to no sources!")

    initial_source = sources[0]
    meta = await ctx.dispatcher.async_submit_binary(initial_source, label, data, SUBMIT_BINARY_TIMEOUT_SECONDS)

    for additional_source in sources[1:]:
        ctx.dispatcher.copy_binary(initial_source, additional_source, label, meta.sha256)

    return meta


async def _process_augmented_streams(
    ctx: context.Context, sources_to_submit: list[str], augstreams: list[tuple[str, UploadFile]]
) -> list[azm.Datastream]:
    """Process all the aug streams as a single async task."""
    aug_tasks: dict[asyncio.Task[azm.Datastream]] = {}
    # process all the aug streams.
    async with asyncio.TaskGroup() as tg:
        for label, aug_binary in augstreams or []:
            if label == "content":
                raise ApiException(
                    status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                    ref="aug stream cannot be 'content'",
                    external="aug stream cannot be 'content'",
                    internal="upload_no_aug_content",
                )

            augTask = tg.create_task(_submit_binary_data_with_sources(ctx, sources_to_submit, label, aug_binary))
            aug_tasks[label] = augTask

    # Now all the aug
    augstream_meta = []
    for label, completed_task in aug_tasks.items():
        aug_meta = completed_task.result()
        aug_meta.label = label
        augstream_meta.append(aug_meta)

    return augstream_meta


def _submit_binary_event(
    *,
    author: azm.Author,
    entity: azm.BinaryEvent.Entity,
    filename: str,
    source: str,
    timestamp: str,
    references: dict[str, str],
    submit_settings: dict[str, str],
    security: str,
    ctx: context.Context,
    priv_ctx: context.Context,
    expedite: bool,
):
    """Submit an event to the dispatcher."""
    # check that source is valid
    if not settings.check_source_exists(source):
        raise ApiException(
            status_code=HTTP_400_BAD_REQUEST,
            ref=f"Source is not defined: {source}",
            internal="bad_source_id",
        )
    try:
        settings.check_source_references(source, references)
    except settings.BadSourceRefsException as e:
        raise ApiException(
            status_code=HTTP_400_BAD_REQUEST,
            ref=str(e),
            internal="bad_source_refs",
        )

    author.security = security

    # Remove on any event beyond the inital submission as, always 1 on a sourced event.
    if submit_settings:
        submit_settings[binary_submit_manual.SUBMIT_SETTINGS_DEPTH_REMOVAL_KEY] = "2"
    event_details = azm.BinaryEvent(
        kafka_key="meta-tmp",  # temporary id so we can create the object
        action=azm.BinaryAction.Sourced,
        model_version=azm.CURRENT_MODEL_VERSION,
        timestamp=timestamp,
        author=author,
        entity=entity,
        source=azm.Source(
            name=source,
            timestamp=timestamp,
            references=references,
            settings=submit_settings,
            path=[
                azm.PathNode(
                    author=author,
                    action=azm.BinaryAction.Sourced,
                    timestamp=timestamp,
                    sha256=entity.sha256,
                    filename=data_common.basename(filename) if filename else None,
                    size=entity.size,
                    file_format_legacy=entity.file_format_legacy,
                    file_format=entity.file_format,
                )
            ],
            security=security,
        ),
    )
    # set fake ID as dispatcher will generate a proper one
    event_details.kafka_key = "tmp"
    submission = [event_details]
    if expedite:
        # generate deep copy with the expedite flag set
        ev_expedited = azm.BinaryEvent(**event_details.model_dump())
        ev_expedited.flags.expedite = True
        submission.append(ev_expedited)

    # send to dispatcher and get enhanced copy of events
    resp = ctx.dispatcher.submit_events(submission, model=azm.ModelType.Binary, include_ok=True)

    # index to metastore immediately
    # slow - skip this for automated submissions
    if expedite:
        try:
            # write events immediately
            binary_create.create_binary_events(
                priv_ctx,
                [azm.BinaryEvent(**x) for x in resp.ok],
                immediate=True,
            )
        except Exception as e:
            raise ApiException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                ref="Unable to submit binary event to metastore immediately",
                internal=str(e),
            ) from e

    if len(resp.ok) == 0:
        raise ApiException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            ref="Dispatcher rejected the submitted events",
            internal=str(resp),
        )
    model = basic_events.BinaryEvent(**resp.ok[0])
    return model


async def high_level_submit_binary(
    ctx: context.Context,
    priv_ctx: context.Context,
    *,
    binary: UploadFile | None = None,
    sha256: str = "",
    source: str = "",
    references: dict | None = None,
    parent_sha256: str = "",
    relationship: dict | None = None,
    submit_settings: dict | None = None,
    filename: str = "",
    timestamp: str = "",
    security: str = "",
    extract: bool = False,
    password: str = "",
    user: str = "",
    augstreams: list[tuple[str, UploadFile]] | None = None,
    expedite: bool = False,
) -> list[bedr_bdata.BinaryData]:
    """Submit a binary to Azul."""
    if references is None:
        references = dict()
    if relationship is None:
        relationship = dict()
    if submit_settings is None:
        submit_settings = dict()

    try:
        ctx.azsec.check_access(ctx.get_user_access().security.labels, security, raise_error=True)
    except SecurityParseException:
        raise ApiException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            ref="Must provide valid security string.",
            internal="invalid_security_string",
        )
    except SecurityAccessException as e:
        raise ApiException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            ref="security greater than user permissions",
            external="security being applied by the user is greater than the current users security."
            + f"because user: {str(e)}",
            internal="security_too_secure",
        ) from e

    if not parent_sha256 and not source:
        raise ApiException(
            status_code=HTTP_400_BAD_REQUEST,
            ref="Must provide source or parent information.",
            internal="no_parent_and_source_submitted",
        )

    if parent_sha256 and source:
        raise ApiException(
            status_code=400,
            ref="cannot insert binary to source and parent at same time",
            internal="parent_and_source_both_submitted",
        )

    if not binary and not sha256:
        raise ApiException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            ref="Must supply binary or sha256",
            external="Must supply binary or sha256",
            internal="upload_no_binary_sha256",
        )

    if parent_sha256 and not binary_read.find_stream_references(ctx, parent_sha256)[0]:
        raise ApiException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            ref="Parent Id (sha256) must already exist",
            external="Parent Id (sha256) must already exist",
            internal="upload_not_found_parent_sha256",
        )

    is_source_submission = bool(source)
    author = azm.Author(category="user", name=user)

    sources_to_submit = []
    if is_source_submission:
        sources_to_submit.append(source)
    else:
        # attaching to parent, query opensearch to get list of sources
        sources_to_submit = binary_read.list_all_sources_for_binary(ctx, parent_sha256)

    augstream_meta = await _process_augmented_streams(ctx, sources_to_submit, augstreams)

    entities: list[azm.BinaryEvent.Entity] = []
    if binary:
        try:
            async for data, fname in fileformat.unpack_content(binary, extract, password):
                # archives always need to use internal filename
                # non-archives always need to use provided filename
                local_filename = fname if extract else filename
                # submit file to dispatcher and transform returned metadata
                binary_details = await _submit_binary_data_with_sources(
                    ctx, sources_to_submit, azm.DataLabel.CONTENT, data
                )
                # the dispatcher sets the label - override it here
                # FUTURE make dispatcher set this label
                binary_details.label = azm.DataLabel.CONTENT
                entity = _transform_metadata_to_binary_entity(
                    binary_details, local_filename, augstreams=augstream_meta
                )
                entities.append(entity)
        except fileformat.ExtractException as e:
            raise ApiException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                ref="bad_bundled_submission",
                external=str(e),
                internal=str(e),
            )
    else:
        # dataless submission
        # this query limits docs to the callers permissions
        # so the hash may exist in the system but the error is raised anyway
        try:
            doc = next(binary_submit_dataless.stream_dispatcher_events_for_binary(ctx, sha256))
        except StopIteration:
            raise ApiException(
                status_code=HTTP_404_NOT_FOUND,
                ref="Unable to find existing metadata",
                external=f"Cannot find existing metadata for entity {sha256}. "
                "You will need to supply Azul with the original binary.",
                internal="upload_no_existing_metadata",
            )
        # first entry in data block must have been generated by dispatcher for 'content'
        binary_details = azm.BinaryEvent(**doc).entity.datastreams[0]
        entity = _transform_metadata_to_binary_entity(binary_details, filename, augstreams=augstream_meta)
        entities.append(entity)
    if not entities:
        raise ApiException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            ref="Unable to extract files",
            external="Cannot find any files to extract. This may be an unsupported filetype or bad password.",
            internal="upload_nothing_extracted",
        )

    # Submit events to dispatcher and track the returned submission data.
    return_val: list[bedr_bdata.BinaryData] = []

    for entity in entities:
        # Indexing first element because that is where we store the main content with the label "content".
        # This is the main binary that has been uploaded for the event in question and is the metadata we want.
        new_model = bedr_bdata.BinaryData(**entity.datastreams[0].model_dump())

        # get the filename again
        filenames = [x.value for x in entity.features if x.name == "filename"]
        filename = filenames[0] if filenames else None

        if filename:
            new_model.filename = filename

        if is_source_submission:
            # build up a submission event
            return_model = _submit_binary_event(
                author=author,
                entity=entity,
                filename=filename,
                source=source,
                timestamp=timestamp,
                references=references,
                submit_settings=submit_settings,
                security=security,
                ctx=ctx,
                priv_ctx=priv_ctx,
                expedite=expedite,
            )
            new_model.track_source_references = return_model.track_source_references
        else:
            # parent-child insert
            binary_submit_manual.submit(
                author=author,
                original_source=sources_to_submit[0],
                parent_sha256=parent_sha256,
                entity=entity,
                timestamp=timestamp,
                relationship=relationship,
                submit_settings=submit_settings,
                filename=filename,
                security=security,
                ctx=ctx,
                priv_ctx=priv_ctx,
                expedite=expedite,
            )
        return_val.append(new_model)

    return return_val
