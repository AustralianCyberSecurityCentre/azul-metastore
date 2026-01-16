"""Submit or trigger processing for binaries."""

import json
import logging
from typing import Annotated, Optional, Self

from azul_bedrock import models_network as azm
from azul_bedrock.exceptions import ApiException, BaseError
from azul_bedrock.models_restapi import binaries_data as bedr_binaries_data
from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Path,
    Query,
    Request,
    Response,
    UploadFile,
)
from pydantic import AfterValidator, BaseModel, computed_field, model_validator
from starlette.status import HTTP_422_UNPROCESSABLE_CONTENT

from azul_metastore import context
from azul_metastore.common.utils import to_utc
from azul_metastore.query.binary2 import binary_expedite, binary_submit
from azul_metastore.restapi.quick import qr

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(name)s:%(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    level=logging.WARNING,
)


def _to_utc_except(timestamp: str):
    """Convert timestamp to UTC and raise api exception if not valid."""
    try:
        return to_utc(timestamp)
    except Exception as e:
        raise ApiException(
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
            ref="bad_timestamp",
            external="a bad timestamp was provided",
            internal="bad_timestamp",
        ) from e


def _log_high_level_submission(
    request: Request, ctx: context.Context, high_level_sub_response: list[bedr_binaries_data.BinaryData]
):
    """Log uploading files to various sources."""
    for submitted_data in high_level_sub_response:
        ctx.man.s.log_to_loki(ctx.user_info.username, request, submitted_data.sha256)


def validate_json(field_name: str, field_value: str | None):
    """Validate that the provided field_value is valid json."""
    try:
        # Only validate the json field if the value is set (not None or an empty string)
        if field_value:
            json.loads(field_value)
    except ValueError as e:
        raise ApiException(
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
            ref="bad_json",
            external=f"Bad json for the field {field_name} was provided, value was {field_value}",
            internal="bad_json",
        ) from e


class SubmissionSettings(BaseModel):
    """Submission Settings Json settings."""

    settings: str | None = None

    @computed_field
    @property
    def settings_as_dict(self) -> dict[str, str]:
        """Convert the json field to a dictionary."""
        if not self.settings:
            return {}
        return json.loads(self.settings)

    @model_validator(mode="after")
    def verify_json_is_valid(self) -> Self:
        """Verify the model is valid by checking the settings json is valid."""
        validate_json("settings", self.settings)
        return self

    @classmethod
    def as_form(
        cls,
        settings: str | None = Form(
            None,
            description='JSON Key:Value pairs to add additional options to a submission e.g {"passwords": "abc;def"}',
        ),
    ):
        """Get the form representation of this model."""
        return cls(settings=settings)


class SubmissionReferences(BaseModel):
    """Submission References Json Form validation."""

    references: str | None = None

    @computed_field
    @property
    def references_as_dict(self) -> dict[str, str]:
        """Convert the json field to a dictionary."""
        if not self.references:
            return {}
        return json.loads(self.references)

    @model_validator(mode="after")
    def verify_json_is_valid(self) -> Self:
        """Verify the model is valid by checking the reference json is valid."""
        validate_json("references", self.references)
        return self

    @classmethod
    def as_form(
        cls,
        references: str | None = Form(
            None, description='JSON Key:Value pairs to label binary (source specific) e.g {"user":"anon@email"}'
        ),
    ):
        """Get the form representation of this model."""
        return cls(references=references)


class SubmissionRelationship(BaseModel):
    """Submission Relationship Json validation."""

    relationship: str | None = None

    @computed_field
    @property
    def relationship_as_dict(self) -> dict[str, str]:
        """Convert the json field to a dictionary."""
        if not self.relationship:
            return {}
        return json.loads(self.relationship)

    @model_validator(mode="after")
    def verify_json_is_valid(self) -> Self:
        """Verify the model is valid by checking the relationship json is valid."""
        validate_json("relationship", self.relationship)
        return self

    @classmethod
    def as_form(
        cls, relationship: str | None = Form(None, description="JSON Key:Value pairs to label parent-child relation")
    ):
        """Get the form representation of this model."""
        return cls(relationship=relationship)


class CommonBinarySubmitFormParams(BaseModel):
    """Common form parameters for submissions."""

    # metadata filename
    filename: Optional[str]
    # might seem odd we have a filename property since filename is part of UploadFile
    # * requests uses 'upload' as the default filename if none is supplied on a POST
    # * requests or fastapi cannot handle an empty string as a filename
    timestamp: Annotated[str, AfterValidator(_to_utc_except)]
    security: str

    @classmethod
    def as_form(
        cls,
        filename: Optional[str] = Form(None, description="Filename to include in submission"),
        # Elipses (...) means Required.
        timestamp: str = Form(..., description="Date the file was sourced in ISO8601 format"),
        security: str = Form(
            "",
            description="Space separated list of security labels e.g 'OFFICIAL TLP:CLEAR'",
        ),
    ):
        """Convert the pydantic model into a form."""
        return cls(filename=filename, timestamp=timestamp, security=security)


@router.post(
    "/v0/binaries/source",
    response_model=list[bedr_binaries_data.BinaryData],
    responses={
        422: {"model": BaseError, "description": "Invalid file"},
    },
    response_model_exclude_unset=True,
)
async def submit_binary_to_source(
    request: Request,
    resp: Response,
    # data submission
    binary: UploadFile = File(description="File to submit"),
    # source submission
    source_id: str = Form(description="Source/grouping to submit the file into"),
    references: SubmissionReferences = Depends(SubmissionReferences.as_form),
    settings: SubmissionSettings = Depends(SubmissionSettings.as_form),
    # archives
    extract: bool = Query(False, description="Extract entities from archive before processing"),
    password: str = Query(None, description="Password used for neutering"),
    # augstreams
    stream_data: list[UploadFile] = File([], description="alternate streams for main binary"),
    stream_labels: list[azm.DataLabel] = Form([], description="labels for alternate streams"),
    # metadata
    commonMeta: CommonBinarySubmitFormParams = Depends(CommonBinarySubmitFormParams.as_form),
    # misc
    refresh: bool = Query(
        False, description="Make data and plugin results available as quickly as possible (slower upload rate)."
    ),
    ctx: context.Context = Depends(qr.ctx),
):
    """Submit a binary file to source for analysis.

    Metadata included in the CaRT/MalPZ file is discarded.

    If an archive is supplied along with stream_data, that stream data will be added to all archive contents.

    Valid file formats:

        raw data
        CaRT
        MalPZ
        TAR (extract=true)
        ZIP (extract=true)
    """
    # Validate user supplied label
    security = ctx.validate_user_security(commonMeta.security)

    try:
        user = request.state.user_info.username

        # read alt streams
        if len(stream_data) != len(stream_labels):
            raise ApiException(
                status_code=HTTP_422_UNPROCESSABLE_CONTENT,
                ref="stream labels must be supplied for all stream data",
                external="stream labels must be supplied for all stream data",
                internal="upload_bad_stream_data_labels",
            )
        augstreams = []
        for stream, label in zip(stream_data, stream_labels):
            augstreams.append((label, stream))

        qr.set_security_headers(ctx, resp, security)
        result = await binary_submit.high_level_submit_binary(
            binary=binary,
            source=source_id,
            references=references.references_as_dict,
            submit_settings=settings.settings_as_dict,
            filename=commonMeta.filename,
            timestamp=commonMeta.timestamp,
            security=security,
            extract=extract,
            password=password,
            user=user,
            ctx=ctx,
            priv_ctx=qr.writer,
            augstreams=augstreams,
            expedite=refresh,
        )
        _log_high_level_submission(request, ctx, result)
        return result
    except HTTPException as e:
        qr.set_security_headers(ctx, resp, security, ex=e)
        raise


@router.post(
    "/v0/binaries/source/dataless",
    response_model=list[bedr_binaries_data.BinaryData],
    responses={
        422: {"model": BaseError, "description": "Invalid file"},
    },
    response_model_exclude_unset=True,
)
async def submit_binary_to_source_dataless(
    request: Request,
    resp: Response,
    # dataless submission
    sha256: str = Form(None, description="sha256 of file if binary data not present", pattern="[a-fA-F0-9]{64}"),
    # source submission
    source_id: Optional[str] = Form(None, description="Source/grouping to submit the file into"),
    references: SubmissionReferences = Depends(SubmissionReferences.as_form),
    # augstreams
    stream_data: list[UploadFile] = File([], description="alternate streams for main binary"),
    stream_labels: list[azm.DataLabel] = Form([], description="labels for alternate streams"),
    # metadata
    commonMeta: CommonBinarySubmitFormParams = Depends(CommonBinarySubmitFormParams.as_form),
    # misc
    refresh: bool = Query(
        False, description="Make data and plugin results available as quickly as possible (slower upload rate)."
    ),
    ctx: context.Context = Depends(qr.ctx),
):
    """Submit additional metadata about an existing binary.

    Can be used to add things like new alt streams or a new source for a binary.
    """
    # Validate user supplied label
    security = ctx.validate_user_security(commonMeta.security)

    try:
        user = request.state.user_info.username

        # read alt streams
        if len(stream_data) != len(stream_labels):
            raise ApiException(
                status_code=HTTP_422_UNPROCESSABLE_CONTENT,
                ref="stream labels must be supplied for all stream data",
                external="stream labels must be supplied for all stream data",
                internal="upload_bad_stream_data_labels",
            )
        augstreams = []
        for stream, label in zip(stream_data, stream_labels):
            augstreams.append((label, stream))

        qr.set_security_headers(ctx, resp, security)
        result = await binary_submit.high_level_submit_binary(
            sha256=sha256,
            source=source_id,
            references=references.references_as_dict,
            filename=commonMeta.filename,
            timestamp=commonMeta.timestamp,
            security=security,
            user=user,
            ctx=ctx,
            priv_ctx=qr.writer,
            augstreams=augstreams,
            expedite=refresh,
        )
        _log_high_level_submission(request, ctx, result)
        return result
    except HTTPException as e:
        qr.set_security_headers(ctx, resp, security, ex=e)
        raise


@router.post(
    "/v0/binaries/child",
    response_model=list[bedr_binaries_data.BinaryData],
    responses={
        422: {"model": BaseError, "description": "Invalid file"},
    },
    response_model_exclude_unset=True,
)
async def submit_child_binary_to_source(
    request: Request,
    resp: Response,
    # data submission
    binary: UploadFile = File(description="File to submit"),
    # child submission
    parent_sha256: str = Form(description="Parent entity id", pattern="[a-fA-F0-9]{64}"),
    relationship: SubmissionRelationship = Depends(SubmissionRelationship.as_form),
    settings: SubmissionSettings = Depends(SubmissionSettings.as_form),
    # archives
    extract: bool = Query(False, description="Extract entities from archive before processing"),
    password: str = Query(None, description="Password used for neutering"),
    # metadata
    commonMeta: CommonBinarySubmitFormParams = Depends(CommonBinarySubmitFormParams.as_form),
    refresh: bool = Query(
        False, description="Make data and plugin results available as quickly as possible (slower upload rate)."
    ),
    ctx: context.Context = Depends(qr.ctx),
):
    """Submit a child binary to a parent binary for analysis."""
    # Validate user supplied label
    security = ctx.validate_user_security(commonMeta.security)

    try:
        user = request.state.user_info.username

        qr.set_security_headers(ctx, resp, security)
        result = await binary_submit.high_level_submit_binary(
            binary=binary,
            parent_sha256=parent_sha256,
            relationship=relationship.relationship_as_dict,
            submit_settings=settings.settings_as_dict,
            filename=commonMeta.filename,
            timestamp=commonMeta.timestamp,
            security=security,
            extract=extract,
            password=password,
            user=user,
            ctx=ctx,
            priv_ctx=qr.writer,
            expedite=refresh,
        )
        _log_high_level_submission(request, ctx, result)
        return result
    except HTTPException as e:
        qr.set_security_headers(ctx, resp, security, ex=e)
        raise


@router.post(
    "/v0/binaries/child/dataless",
    response_model=list[bedr_binaries_data.BinaryData],
    responses={
        422: {"model": BaseError, "description": "Invalid file"},
    },
    response_model_exclude_unset=True,
)
async def submit_child_binary_to_source_dataless(
    request: Request,
    resp: Response,
    # dataless submission
    sha256: str = Form(description="sha256 of file if binary data not present", pattern="[a-fA-F0-9]{64}"),
    # child submission
    parent_sha256: str = Form(description="Parent entity id"),
    relationship: SubmissionRelationship = Depends(SubmissionRelationship.as_form),
    # metadata
    commonMeta: CommonBinarySubmitFormParams = Depends(CommonBinarySubmitFormParams.as_form),
    refresh: bool = Query(
        False, description="Make data and plugin results available as quickly as possible (slower upload rate)."
    ),
    ctx: context.Context = Depends(qr.ctx),
):
    """Submit child metadata and attach it to an existing parent binary.

    Typically used to create a relationship between two existing binaries where there was none before.
    """
    # Validate user supplied label
    security = ctx.validate_user_security(commonMeta.security)

    try:
        user = request.state.user_info.username

        qr.set_security_headers(ctx, resp, security)
        result = await binary_submit.high_level_submit_binary(
            sha256=sha256,
            parent_sha256=parent_sha256,
            relationship=relationship.relationship_as_dict,
            filename=commonMeta.filename,
            timestamp=commonMeta.timestamp,
            security=security,
            user=user,
            ctx=ctx,
            priv_ctx=qr.writer,
            expedite=refresh,
        )
        _log_high_level_submission(request, ctx, result)
        return result
    except HTTPException as e:
        qr.set_security_headers(ctx, resp, security, ex=e)
        raise


@router.post("/v0/binaries/{sha256}/expedite")
async def expedite_processing(
    resp: Response,
    sha256: str = Path(..., pattern="[a-fA-F0-9]{64}", description="SHA256 of entity to expedite"),
    bypass_cache: bool = Query(False),
    ctx: context.Context = Depends(qr.ctx),
):
    """Trigger an entity to be (re)processed at a higher priority than normal."""
    # This doesn't tell the user about the existence of a given file, so it is
    # safe to return the default security label for the system (even if the given
    # file is not that security label)
    security = ctx.azsec.get_default_security()
    try:
        qr.set_security_headers(ctx, resp, security)
        binary_expedite.expedite_processing(ctx, qr.writer, sha256, bypass_cache)
        return True
    except HTTPException as e:
        qr.set_security_headers(ctx, resp, security, ex=e)
