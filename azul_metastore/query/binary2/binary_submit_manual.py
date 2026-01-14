"""Queries to assist with manual child submissions."""

import logging
from typing import Iterable

from azul_bedrock import models_network as azm
from azul_bedrock.exceptions import ApiException
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from azul_metastore import context
from azul_metastore.common import data_common
from azul_metastore.encoders import binary2 as rc
from azul_metastore.query import binary_create

logger = logging.getLogger(__name__)

SUBMIT_SETTINGS_DEPTH_REMOVAL_KEY = "remove_at_depth"


def _stream_events_for_manual_submission(
    ctx: context.Context,
    sha256: str,
) -> Iterable[dict]:
    """Read result documents relevant for manual submission of a child.

    For every source the parent appears in, retrieve
    * up to 100 events with security variations
    * up to 100 events with source reference variations
    """
    sha256 = sha256.lower()
    body = {
        "_source": False,
        "size": 100,  # assumption that we have less than 100 sources
        "collapse": {
            "field": "source.name",
            "inner_hits": [
                {"name": "securities", "collapse": {"field": "security"}, "size": 100},
                {"name": "references", "collapse": {"field": "track_source_references"}, "size": 100},
            ],
        },
        "query": {
            "bool": {
                "filter": [
                    {"term": {"sha256": sha256}},
                    {"exists": {"field": "source"}},
                ],
            }
        },
    }

    seen = set()
    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    for outer in resp["hits"]["hits"]:
        for collapse in ["references", "securities"]:
            for inner in outer["inner_hits"][collapse]["hits"]["hits"]:
                src = inner["_source"]
                if inner["_id"] in seen:
                    # probably saw this already from the other collapse collection
                    continue
                seen.add(inner["_id"])
                src["_index"] = inner["_index"]
                src["_id"] = inner["_id"]

                yield rc.Binary2.decode(src)


def _stream_append_manual_insert(
    ctx: context.Context, child_event: dict, submit_settings: dict[str, str]
) -> Iterable[azm.BinaryEvent]:
    """Read all events for parent and propagate to new child."""
    parent_sha256 = child_event["entity"]["parent_sha256"].lower()
    for row in _stream_events_for_manual_submission(ctx, parent_sha256):
        # add child node to parents path and assemble child event
        node = child_event["entity"]["child_history"]
        row["source"]["path"].append(node)
        # ensure tracking info is updated
        row.setdefault("track_authors", []).append(child_event["track_author"])
        row.setdefault("track_links", []).append(child_event["track_link"])
        event = azm.BinaryEvent(
            model_version=azm.CURRENT_MODEL_VERSION,
            kafka_key="meta-tmp",
            timestamp=child_event["timestamp"],
            entity=child_event["entity"]["child"],
            source=row["source"],
            action=node["action"],
            author=child_event["author"],
            track_source_references=row["track_source_references"],
            track_links=row["track_links"],
            track_authors=row["track_authors"],
        )
        # Add source settings to the submission.
        event.source.settings = submit_settings
        # Settings should be removed at one more than initial depth so it only affects, first round of plugin results.
        if event.source.settings:
            event.source.settings[SUBMIT_SETTINGS_DEPTH_REMOVAL_KEY] = str(len(event.source.path) + 1)
        yield event


def submit(
    *,
    author: azm.Author,
    original_source: str,
    parent_sha256: str,
    entity: azm.BinaryEvent.Entity,
    security: str,
    relationship: dict,
    submit_settings: dict[str, str],
    timestamp: str,
    filename: str,
    ctx: context.Context,
    priv_ctx: context.Context,
    expedite: bool,
):
    """User triggered insertion of an entity as child of another entity."""
    author.security = security
    params = {"name": "metastore-insert", "version": "2021-03-19"}

    if filename:
        filename = data_common.basename(filename)

    event = azm.InsertEvent(
        kafka_key="meta-tmp",
        model_version=azm.CURRENT_MODEL_VERSION,
        author=author,
        entity=azm.InsertEvent.Entity(
            original_source=original_source,
            parent_sha256=parent_sha256,
            child=entity,
            child_history=azm.PathNode(
                author=author,
                action=azm.BinaryAction.Extracted,
                sha256=entity.sha256,
                filename=filename,
                size=entity.size,
                file_format=entity.file_format,
                relationship=relationship,
                timestamp=timestamp,
            ),
        ),
        timestamp=timestamp,
    )

    # send to dispatcher and get enhanced copy of events
    # as this is not a binary event, there is nothing to expedite
    resp = ctx.dispatcher.submit_events([event], model=azm.ModelType.Insert, params=params, include_ok=True)
    if len(resp.ok) == 0:
        raise ApiException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            ref="Dispatcher rejected the submitted events",
            internal=str(resp),
        )
    full_event = resp.ok[0]

    # calculate required binary events and send through to dispatcher for processing
    binary_events = list(_stream_append_manual_insert(priv_ctx, full_event, submit_settings))
    logger.debug(f"creating manual insertion events: {len(binary_events)}")
    resp = ctx.dispatcher.submit_events(binary_events, model=azm.ModelType.Binary, params=params, include_ok=True)

    if expedite:
        # we need the metastore to propagate through existing records
        try:
            binary_create.create_binary_events(priv_ctx, [azm.BinaryEvent(**x) for x in resp.ok])
            priv_ctx.refresh()
        except Exception as e:
            raise ApiException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                ref="Unable to propagate insert events to metastore",
                internal=str(e),
            ) from e
