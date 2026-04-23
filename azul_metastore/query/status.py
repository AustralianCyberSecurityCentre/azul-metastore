"""Queries for finding status data."""

import logging
import traceback
from collections import defaultdict

import pendulum
from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi
from prometheus_client import Counter
from pydantic import BaseModel

from azul_metastore.common.query_info import IngestError
from azul_metastore.common.utils import capture_write_stats, get_author_from_generic_event
from azul_metastore.context import Context
from azul_metastore.encoders import status as st
from azul_metastore.models import basic_events
from azul_metastore.query import plugin
from azul_metastore.query.binary2 import binary_event

duplicate_status_id = Counter(
    "azul_ingest_duplicate_id", "Ingestion events dropped that had duplicate Ids.", ["type", "status", "plugin"]
)

logger = logging.getLogger(__name__)


def get_statuses(ctx: Context, sha256: str) -> list[dict]:
    """Return all statuses for entity, intended for debugging purposes only.

    :param ctx: context
    :param sha256: id of binary to find statuses off
    :param format: type of entity e.g. 'binary'
    :return: list of statuses
    """
    sha256 = sha256.lower()
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"entity.input.entity.sha256": sha256}},
                ]
            }
        },
        "size": 10000,
    }
    resp = ctx.man.status.w.search(ctx.sd, body=body)
    results = [st.Status.decode(x["_source"]) for x in resp["hits"]["hits"]]
    results.sort(key=lambda x: x["timestamp"])
    return results


def _get_opensearch_binary_status(ctx: Context, sha256: str) -> list[models_restapi.StatusEvent]:
    """Return most recent statuses for all authors.

    :param ctx: context
    :param sha256: id of binary to find statuses of
    :param format: type of entity e.g. 'binary'
    :return: list of statuses
    """
    sha256 = sha256.lower()
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"entity.input.entity.sha256": sha256}},
                ]
            }
        },
        "aggs": {
            "author": {
                "terms": {"field": "encoded.author", "size": 10000},
                "aggs": {
                    "hit": {
                        "top_hits": {
                            "size": 1,
                            "sort": [{"timestamp": "desc"}],
                            "_source": {
                                "include": ["timestamp", "author", "security", "encoded", "entity"],
                                "exclude": ["entity.input.source.path"],
                            },
                        }
                    }
                },
            },
            "count": {
                "filter": {"terms": {"entity.status": [x.value for x in azm.StatusEnumSuccess]}},
                "aggs": {"count_2": {"terms": {"field": "encoded.author", "size": 10000}}},
            },
        },
        "size": 0,
    }
    resp = ctx.man.status.w.search(ctx.sd, body=body)
    count = {}
    for hit in resp["aggregations"]["count"]["count_2"]["buckets"]:
        count[hit["key"]] = hit["doc_count"]

    results = []
    for b in resp["aggregations"]["author"]["buckets"]:
        for hit in b["hit"]["hits"]["hits"]:
            status = hit["_source"]
            # Get count of complete messages
            status["completed"] = count.get(status["encoded"]["author"], 0)
            results.append(models_restapi.StatusEvent(**st.Status.decode(status)))

    return results


def get_binary_status(ctx: Context, sha256: str) -> list[models_restapi.StatusEvent]:
    """Determine current status of processing for all plugins with the binary."""
    sha256 = sha256.lower()
    map_statuses = {}

    # read out which plugins should exist in the system
    all_plugins = plugin.get_all_plugins_full(ctx)
    for row in all_plugins:
        k = f"{row.author.name}-{row.author.version}"
        # add inactive entries, this means the plugin registered at some point
        # since dispatcher keeps consumers for 1 day, this are probably not wanted in the final result
        # i.e. maco deployments that change pretty frequently, we don't want all last months versions reported
        map_statuses[k] = models_restapi.StatusEvent(
            timestamp=row.timestamp,
            entity=models_restapi.StatusEntity(
                input=models_restapi.StatusInput(
                    entity=models_restapi.StatusInputEntity(sha256=sha256),
                ),
                status="inactive",
                runtime=0,
            ),
            completed=0,
            security=row.security,
            author=row.author,
        )

    # enrich if plugins should be processing the binary
    best_event = binary_event.get_best_event(ctx, sha256)
    if best_event:
        simulated = ctx.dispatcher.simulate_consumers_on_event(best_event)
        for row in simulated.consumers:
            k = f"{row.name}-{row.version}"
            if k not in map_statuses:
                continue
            # fake status to represent complex situations
            status = "prefiltered" if row.filter_out else "queued"
            map_statuses[k] = models_restapi.StatusEvent(
                timestamp=best_event.timestamp.isoformat(),
                entity=models_restapi.StatusEntity(
                    input=models_restapi.StatusInput(
                        entity=models_restapi.StatusInputEntity(sha256=sha256),
                    ),
                    status=status,
                    runtime=0,
                    message=row.filter_out_trigger,
                ),
                completed=0,
                security=map_statuses[k].security,
                author=map_statuses[k].author,
            )

    # enrich if plugins are currently or have processed the binary
    processed = _get_opensearch_binary_status(ctx, sha256)
    for row in processed:
        k = f"{row.author.name}-{row.author.version}"
        map_statuses[k] = row

    # convert to list and drop 'inactive' entries - we probably don't want these in final results
    statuses = [x for x in map_statuses.values() if x.entity.status != "inactive"]
    # if we haven't seen a heartbeat for 2 minutes, edit to be 'heartbeart-lost' fake status
    for status in statuses:
        if status.entity.status != "heartbeat":
            continue
        if pendulum.now(pendulum.UTC).subtract(minutes=2) > pendulum.parse(status.timestamp):  # type: ignore
            # timed out
            status.entity.status += "-lost"
            status.entity.error = "more than 2 minutes after last heartbeat"

    statuses.sort(key=lambda x: x.author.name.lower())
    return statuses


@capture_write_stats("status")
def create_status(
    ctx: Context, raw_events: list[azm.StatusEvent]
) -> tuple[list[IngestError], list[azm.StatusEvent], dict[str, int]]:
    """Save list of statuses to opensearch.

    :param ctx: Context
    :param raw_results: Results to be saved
    :return tuple[list[dict], list[dict]]: a tuple with the bad_results and the duplicate results in seperate lists.
    """
    results: dict[str, dict] = dict()
    bad_raw_results: list[IngestError] = []
    duplicate_docs: list[azm.StatusEvent] = []
    # Reverse raw_results so if there are duplicate ids we get the newest event.
    for raw_event in reversed(raw_events):
        try:
            normalised = basic_events.StatusEvent.normalise(raw_event)
            encoded = st.Status.encode(normalised)
        except Exception as e:
            # retain error and process other events
            bad_raw_results.append(
                IngestError(
                    doc=raw_event, error_type=e.__class__.__name__, error_reason=str(e) + "\n" + traceback.format_exc()
                )
            )
            continue
        key_to_add = encoded["_id"]
        if key_to_add in results:
            # Append the raw_event as there is a duplicate.
            duplicate_docs.append(raw_event)
            # Check if the existing data is newer than the data to be added
            # If it is keep the old data and drop the new data.
            time_existing = pendulum.parse(results[key_to_add]["timestamp"])
            time_new = pendulum.parse(encoded["timestamp"])
            if (
                isinstance(time_existing, pendulum.DateTime)
                and isinstance(time_new, pendulum.DateTime)
                and time_existing >= time_new
            ):
                logger.debug(f"There are duplicate document keys when encoding status events id: '{key_to_add}'")
                continue
        results[key_to_add] = encoded
    # No models are valid, nothing to send to opensearch.
    if not results:
        return bad_raw_results, duplicate_docs, dict()

    doc_errors = ctx.man.status.w.wrap_and_index_docs(ctx.sd, results.values(), raise_on_errors=False)

    author_results: dict[str, int] = defaultdict(int)
    for r in results.values():
        author_results[get_author_from_generic_event(r)] += 1
    # Subtract bad author events from expected good ones.
    for brr in doc_errors:
        if isinstance(brr.doc, BaseModel):
            author_results[get_author_from_generic_event(brr.doc.model_dump())] -= 1
        else:
            author_results[get_author_from_generic_event(brr.doc)] -= 1

    return bad_raw_results + doc_errors, duplicate_docs, author_results
