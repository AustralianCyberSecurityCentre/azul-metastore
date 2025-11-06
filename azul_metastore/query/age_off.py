"""Provides functionality to assist with ageing off data from opensearch."""

import logging

import cachetools
import pendulum
from azul_bedrock import models_settings

from azul_metastore import context, settings
from azul_metastore.common import memcache
from azul_metastore.query.binary2 import binary_consistency

logger = logging.getLogger(__name__)


def _cutoff(expire_events_ms: int) -> pendulum.DateTime:
    """Timestamps older than this date are outside the age-off tolerance."""
    if expire_events_ms <= 0:
        # Never cut off anything
        return pendulum.datetime(1970, 1, 1, tz=pendulum.UTC)

    # subtract the duration required from right now
    cutoff = pendulum.now() - pendulum.duration(milliseconds=expire_events_ms)
    return cutoff.in_timezone(pendulum.UTC)


@cachetools.cached(cache=memcache.get_ttl_cache("keep_older_than", ttl=60))
def source_get_cutoff(source: str) -> pendulum.DateTime | None:
    """Return the cutoff timestamp for the source."""
    s = settings.get()
    if s.sources[source].expire_events_ms <= 0:
        # retain all events for this source
        return None
    return _cutoff(s.sources[source].expire_events_ms)


def _should_delete(expire_events_ms: int, most_recent: str) -> bool:
    """Return true if the age off window has been exceeded by the supplied timestamp."""
    # get oldest timestamp we need to keep
    cutoff = _cutoff(expire_events_ms)
    # parse the timestamp
    most_recent_date = pendulum.parse(most_recent).in_tz(pendulum.UTC)
    # if the most recent entry in the index is newer than the cutoff date, should delete
    return most_recent_date < cutoff


def _find_old_indices(
    alias: str, unit: models_settings.PartitionUnitEnum, expire_events_ms: int, field: str
) -> list[str]:
    """Return indices that contain only old events."""
    if unit == models_settings.PartitionUnitEnum.all:
        # Never age off the "all" source
        return []

    ret = []
    ctx = context.get_writer_context()
    # get newest document in each index
    # we calc on 'source.timestamp' - time the top-level source-binary was added
    body = {
        "size": 0,
        "aggs": {
            "index": {
                "terms": {"field": "_index", "size": 1000, "order": {"max_value": "desc"}},
                "aggs": {"max_value": {"max": {"field": field}}},
            }
        },
    }
    resp = ctx.sd.es().search(index=alias, body=body, ignore=[404])
    try:
        if resp.get("error", {}).get("type") == "index_not_found_exception":
            logger.debug(f"{alias=} no indices")
            return ret
        if resp.get("error"):
            logger.debug(f"{alias=} error on query\n{resp}")
            return ret
        if resp["timed_out"]:
            # can't trust the results
            logger.debug(f"{alias=} timed out when querying")
            return ret
        if not resp["hits"]["total"]["value"]:
            return ret
        aggs = resp["aggregations"]["index"]["buckets"]
        # a single alias may resolve to multiple indices
        for b in aggs:
            index = b["key"]
            to_delete = _should_delete(expire_events_ms, b["max_value"]["value_as_string"])
            if not to_delete:
                # if the most recent entry in the index is newer than the cutoff date, skip
                continue

            # exceeds the the age-off period
            ret.append(index)
    except Exception as e:
        raise Exception(f"failed to process ageoff on {alias=} with {expire_events_ms=} response:\n{resp}") from e
    return ret


def _do_age_off_by_query_experimental() -> dict:
    """Run age off via query."""
    ctx = context.get_writer_context()
    alias = ctx.man.binary2.w.alias
    deleted_doc_count = {}

    # delete old submission metadata
    for source, kvs in ctx.man.s.sources.items():
        if kvs.expire_events_ms <= 0:
            # do not age off
            continue
        ms_since_epoch_cutoff = (_cutoff(kvs.expire_events_ms) - pendulum.DateTime.EPOCH).total_seconds() * 1000
        body = {
            "track_total_hits": True,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"source.name": source}},
                        {"range": {"source.timestamp": {"lte": f"{ms_since_epoch_cutoff}"}}},
                    ]
                }
            },
        }
        deleted_doc_count[f"{alias}-submission-{source}"] = ctx.man.binary2.w.delete_loop(ctx.sd, body)

    # ensure db is consistent
    deleted_doc_count[f"{alias}-other"] = binary_consistency.ensure_valid_binaries(ctx)

    return deleted_doc_count


def _find_binaries_for_annotations(ctx: context.Context, search_for_sha256s: set[str]) -> set[str]:
    """Find all the annotations that no longer have a corresponding sha256.

    Return a set of the sha256s that have no documents in the binary index.
    """
    binary_alias = ctx.man.binary2.w.alias

    body = {
        "query": {
            "bool": {
                "filter": [
                    {"terms": {"sha256": list(search_for_sha256s)}},
                ]
            }
        },
        "aggs": {"sha256s": {"terms": {"field": "sha256", "size": 1000}}},
        "size": 0,
    }
    resp = ctx.sd.es().search(index=binary_alias, body=body)
    matching_sha256s = set()
    for bucket_count in resp.get("aggregations").get("sha256s").get("buckets"):
        matching_sha256s.add(bucket_count["key"])

    missing_sha256s = search_for_sha256s - matching_sha256s
    return missing_sha256s


def _do_cleanup_annotations() -> int:
    """Cleanup annotations for binaries or values that no longer exist.

    NOTE - don't ageoff or delete annotations, they should only be disabled as required.
    """
    SCROLL_MAX_DURATION = "1h"
    ctx = context.get_writer_context()
    annotation_alias = ctx.man.annotation.w.alias

    # Look for all annotations with a pivot that aren't deleted.
    body = {
        "query": {
            "bool": {"filter": [{"term": {"type": "entity_tag"}}], "must_not": [{"term": {"state": "disabled"}}]}
        },
        # Only get the sha256 of the annotation, shouldn't need anything else.
        "_source": ["sha256"],
    }
    resp = ctx.sd.es().search(index=annotation_alias, body=body, scroll=SCROLL_MAX_DURATION, ignore=[404])
    scroll_id = resp["_scroll_id"]
    total_hits = resp["hits"]["total"]["value"]
    hits_seen_so_far = 0

    total_annotations_disabled = 0

    try:
        while len(resp["hits"]["hits"]) > 0:
            found_sha256s = set()

            for current_annotation in resp["hits"]["hits"]:
                print(current_annotation)
                annotation_sha256: str = current_annotation["_source"]["sha256"]
                found_sha256s.add(annotation_sha256)

            sha256s_to_remove = _find_binaries_for_annotations(ctx, found_sha256s)
            total_annotations_disabled += len(sha256s_to_remove)
            # Update the annotations as needed to remove them.
            update_body = {
                "script": {"source": "ctx._source.state='disabled'", "lang": "painless"},
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"type": "entity_tag"}},
                            {"terms": {"sha256": list(sha256s_to_remove)}},
                        ],
                        "must_not": [{"term": {"state": "disabled"}}],
                    }
                },
            }
            ctx.sd.es().update_by_query(index=annotation_alias, body=update_body)

            # Avoid additional query if possible.
            hits_seen_so_far += len(resp["hits"]["hits"])
            if hits_seen_so_far >= total_hits:
                break
            resp = ctx.sd.es().scroll(scroll_id=scroll_id, scroll=SCROLL_MAX_DURATION)
    finally:
        # Attempt to clear the scroll even on failures.
        ctx.sd.es().clear_scroll(scroll_id=scroll_id)

    return total_annotations_disabled


def do_age_off() -> tuple[list[str], dict, int]:
    """Do age off for source events and status events.

    Age off indices as a whole first and then documents within indices.
    """
    # Delete all of the old indices
    ctx = context.get_writer_context()
    # get old status indices
    deleted_indices = _find_old_indices(
        ctx.man.status.w.alias,
        ctx.man.s.status_partition_unit,
        ctx.man.s.status_expire_events_ms,
        field="timestamp",  # age off based on when the status was generated
    )

    # get old source indices
    for source, kvs in ctx.man.s.sources.items():
        if kvs.expire_events_ms <= 0:
            # do not age off
            continue
        logger.debug(f"age-off evaluating {source=} with {kvs.partition_unit=} {kvs.expire_events_ms=}")
        # find old indices for the source
        aliases = ctx.man.get_source_aliases(source)
        for alias in aliases:
            # age off based on when the original sourced binary was added to azul
            deleted_indices += _find_old_indices(
                alias, kvs.partition_unit, kvs.expire_events_ms, field="source.timestamp"
            )

    for index in deleted_indices:
        ctx.sd.es().indices.delete(index=index, ignore_unavailable=True, allow_no_indices=True)
        logger.info(f"deleted: {index=}")

    # age off from experimental ingestor
    deleted_docs = _do_age_off_by_query_experimental()
    deleted_docs = {x: y for (x, y) in deleted_docs.items() if y > 0}

    # cleanup any annotations that no longer have a parent.
    deleted_annotations = _do_cleanup_annotations()

    return deleted_indices, deleted_docs, deleted_annotations
