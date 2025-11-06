"""Rerun plugins on the target binary as high priority."""

from typing import Iterable

from azul_bedrock import models_network as azm

from azul_metastore import context
from azul_metastore.common.utils import chunker
from azul_metastore.encoders import binary2 as rc


def _stream_expeditable(
    ctx: context.Context,
    sha256: str,
) -> Iterable[dict]:
    """Read raw binary sourced and extracted documents, useful for resubmissions."""
    sha256 = sha256.lower()
    body = {
        "query": {
            "bool": {
                "should": [
                    {"term": {"action": azm.BinaryAction.Sourced}},
                ],
                "filter": [
                    {"term": {"sha256": sha256}},
                    {"terms": {"action": [azm.BinaryAction.Sourced, azm.BinaryAction.Extracted]}},
                ],
            }
        },
    }

    for resp in ctx.man.binary2.w.scan(ctx.sd, body=body, routing=sha256):
        resp_source = resp["_source"]
        resp_source.pop("_id", None)
        resp_source.pop("_routing", None)
        resp_source.pop("_index", None)
        # Security is taken from sourced information.
        resp_source.pop("security", None)
        resp_source.pop("encoded_security", None)
        yield rc.Binary2.decode(resp_source)


def _yield_expedite_events(ctx: context.Context, sha256: str, bypass_cache: bool):
    """Yield chunks of events for an entity that should be run during an expedite operation."""
    sha256 = sha256.lower()
    for chunk in chunker(_stream_expeditable(ctx, sha256)):
        events = []
        for row in chunk:
            # id is updated by dispatcher
            event = azm.BinaryEvent(kafka_key="tmp", **row)
            event.flags.expedite = True
            event.flags.bypass_cache = bypass_cache
            events.append(event)
        yield events


def expedite_processing(ctx: context.Context, priv_ctx: context.Context, sha256: str, bypass_cache: bool):
    """Trigger an entity to be (re)processed at a higher priority than normal."""
    params = {"name": "metastore-insert", "version": "2021-03-19"}
    chunks = _yield_expedite_events(priv_ctx, sha256, bypass_cache)
    for events in chunks:
        ctx.dispatcher.submit_events(events, model=azm.ModelType.Binary, params=params)
