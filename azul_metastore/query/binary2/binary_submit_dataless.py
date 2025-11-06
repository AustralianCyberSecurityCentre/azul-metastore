"""Queries to assist with dataless submissions."""

from typing import Iterable

from azul_bedrock import models_network as azm

from azul_metastore.context import Context
from azul_metastore.encoders import binary2 as rc


def stream_dispatcher_events_for_binary(
    ctx: Context,
    sha256: str,
) -> Iterable[dict]:
    """Read binary information that has enough info to resubmit under a different entry.

    This is designed for azul-restapi-content to have enough information to
    resubmit an already submitted binary without having the underlying binary data.
    """
    sha256 = sha256.lower()
    # query for specific fields set by dispatcher
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"sha256": sha256}},
                    {"exists": {"field": "datastreams.sha512"}},
                    {"exists": {"field": "datastreams.sha256"}},
                    {"exists": {"field": "datastreams.sha1"}},
                    {"exists": {"field": "datastreams.md5"}},
                    {"exists": {"field": "datastreams.size"}},
                    {"exists": {"field": "datastreams.file_format_legacy"}},
                    {"exists": {"field": "datastreams.magic"}},
                    {"exists": {"field": "datastreams.mime"}},
                    # Ensure we get the sourced event's cnotent datastream back.
                    {"term": {"datastreams.label": azm.DataLabel.CONTENT}},
                    {"exists": {"field": "sha512"}},
                    {"exists": {"field": "sha256"}},
                    {"exists": {"field": "sha1"}},
                    {"exists": {"field": "md5"}},
                    {"exists": {"field": "size"}},
                    {"exists": {"field": "file_format"}},
                    {"exists": {"field": "file_format_legacy"}},
                    {"exists": {"field": "magic"}},
                    {"exists": {"field": "mime"}},
                    # Ensure we get the event that contains content (sourced event instead of augmented events)
                    {"term": {"action": {"value": azm.BinaryAction.Sourced}}},
                ]
            }
        },
    }

    for resp in ctx.man.binary2.w.scan(ctx.sd, body=body, routing=sha256):
        resp["_source"]["kafka_key"] = resp["_id"]
        yield rc.Binary2.decode(resp["_source"])
