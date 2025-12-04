"""Queries for modifying results."""

import logging
import traceback

import pendulum
from azul_bedrock import models_network as azm

from azul_metastore.common.query_info import IngestError
from azul_metastore.common.utils import capture_write_stats
from azul_metastore.context import Context
from azul_metastore.encoders import binary2
from azul_metastore.models import basic_events
from azul_metastore.query import age_off

logger = logging.getLogger(__name__)


def _get_feature_names(events: list[dict]) -> list[str]:
    features = set()
    for event in events:
        keys = event.get("features_map", {}).keys()
        features.update(keys)
    return sorted(features)


def _already_aged_off(event: dict) -> bool:
    """Filter events if timestamp is too old for destination source."""
    # FUTURE this should apply to all events types, not just binary
    source_id = event["source"]["name"]
    cutoff = age_off.source_get_cutoff(source_id)
    if not cutoff:
        # source does not age off so keep the event
        return False
    else:
        # source ages off, compare timestamps
        source_time = event["source"]["timestamp"]
        sourced = pendulum.parse(source_time).in_timezone(pendulum.UTC)
        if sourced > cutoff:
            return False

    return True


@capture_write_stats("binary")
def create_binary_events(
    priv_ctx: Context, raw_events: list[azm.StatusEvent], immediate: bool = False
) -> tuple[list[IngestError], list[azm.BinaryEvent]]:
    """Write binary events to metastore.

    Returns a list of ingest errors and the raw event for duplicate documents.
    """
    return _create_binary_events(priv_ctx, raw_events, immediate)


def _create_binary_events(
    priv_ctx: Context, raw_events: list[azm.BinaryEvent], immediate: bool = False
) -> tuple[list[IngestError], list[azm.BinaryEvent]]:
    """Write binary events to metastore.

    Returns a list of ingest errors and the raw event for duplicate documents.
    """
    results = []
    aged_off = 0
    bad_raw_results = []
    duplicate_docs: list[azm.BinaryEvent] = []

    # Sort the results based on the timestamps to always get newest first,
    # this means if there are any duplicates the newest event is taken.
    for raw_event in sorted(raw_events, key=lambda ev: ev.timestamp, reverse=True):
        try:
            if raw_event.author.name.lower().startswith("maco") or raw_event.author.name.startswith("Maco"):
                logger.info(f"Processing event by {raw_event.author.name} for file {raw_event.entity.sha256}")
                if len(raw_event.entity.features) > 0:
                    logger.info(raw_event.model_dump_json())

            # ensure events are valid
            normalised = basic_events.BinaryEvent.normalise(raw_event)
            # don't write events that should be deleted immediately
            if _already_aged_off(normalised):
                aged_off += 1
                logger.info(f"Auto aging off event by author {raw_event.author.name}+{raw_event.author.version}")
                continue

            # Encode binary events for opensearch indexing
            encoded = binary2.Binary2.encode(normalised)
            filtered_events = binary2.Binary2.filter_seen_and_create_parent_events(encoded)
            # If the event was filtered out during the filtering the event the raw_event is returned.
            # Other events are processed as normal (this is for debugging and stats)
            if len(filtered_events) == 0:
                duplicate_docs.append(raw_event)
            if raw_event.author.name.lower().startswith("maco") or raw_event.author.name.startswith("Maco"):
                logger.info(f"Successfully adding events {len(filtered_events)} - {filtered_events}")
            results.extend(filtered_events)
        except Exception as e:
            # retain error and process other events
            bad_raw_results.append(
                IngestError(
                    doc=raw_event, error_type=e.__class__.__name__, error_reason=str(e) + "\n" + traceback.format_exc()
                )
            )
            continue

    if aged_off > 0:
        logger.info(f"filtered {aged_off} events that were too old for their source")

    # No docs to go to opensearch so stop now.
    if not results:
        return bad_raw_results, duplicate_docs

    # If all results are filtered return immediately.
    if len(results) == 0:
        return bad_raw_results, duplicate_docs

    wrapped = priv_ctx.man.binary2.w.wrap_docs(results)
    # try indexing, and if it fails then ensure that all features are mapped in the template correctly, and retry
    doc_errors = priv_ctx.man.binary2.w.index_docs(priv_ctx.sd, wrapped, refresh=immediate)
    if doc_errors:
        reason = doc_errors[0].error_type
        if reason == "strict_dynamic_mapping_exception":
            feature_names = _get_feature_names(results)
            priv_ctx.man.binary2.w.update_mapping(
                priv_ctx.sd, mapping=priv_ctx.man.binary2.get_mapping_with_features(feature_names)
            )
            doc_errors = priv_ctx.man.binary2.w.index_docs(priv_ctx.sd, wrapped, refresh=immediate)
    if doc_errors:
        return bad_raw_results + doc_errors, duplicate_docs

    return bad_raw_results, duplicate_docs
