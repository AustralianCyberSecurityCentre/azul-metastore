"""Inspect events for raw manipulations."""

import cachetools
from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi

from azul_metastore.common import memcache
from azul_metastore.common.search_query import BINARY_TAG_KEY, FEATURE_TAG_KEY
from azul_metastore.context import Context
from azul_metastore.encoders import binary2 as rc


def get_best_event(ctx: Context, sha256: str) -> azm.BinaryEvent | None:
    """Return the 'best' event for an entity.

    This is specifically intended for dispatcher to evaluate whether consumers would run on an event.
    """
    body = {
        "size": 1,
        "query": {
            "bool": {
                "filter": [{"term": {"sha256": sha256.lower()}}],
                "should": [
                    # must preference events that have content available for dispatcher to process
                    {"term": {"datastreams.label": "content"}},
                    # must preference extracted or sourced events for processing
                    {"terms": {"action": [azm.BinaryAction.Extracted, azm.BinaryAction.Sourced]}},
                ],
            }
        },
        "sort": [
            "_score",  # prioritise based on "should" queries
            {"depth": "asc"},  # prefer non-enriched events
            # Timestamp allows the status events to have the newest sourced/extracted timestamp.
            {"source.timestamp": "desc"},  # tiebreaker
        ],
    }
    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    # check we got anything and decode
    if len(resp["hits"]["hits"]) > 0:
        decoded = rc.Binary2.decode(resp["hits"]["hits"][0]["_source"])
        return azm.BinaryEvent(kafka_key="", **decoded)


def get_binary_documents(
    ctx: Context, sha256: str, event_type: azm.BinaryAction = None, size=1000
) -> models_restapi.OpensearchDocuments:
    """Get raw documents associated with the provided event type."""
    sha256 = sha256.lower()
    body = {
        "track_total_hits": True,
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {"term": {"sha256": sha256}},
                ]
            }
        },
    }
    if event_type:
        body["query"]["bool"]["must"].append({"term": {"action": event_type.value}})
    resp = ctx.man.binary2.w.search(ctx.sd, body=body).get("hits", {})
    return models_restapi.OpensearchDocuments(
        items=resp.get("hits", []), total_docs=resp.get("total", {}).get("value", -1)
    )


def _convert_opensearch_mapping_to_flat(object: dict[str, any]) -> dict[str, str]:
    """Convert an OpenSearch mapping to a flat key:type dictionary."""
    results = {}

    for key, value in object.items():
        if key == "security":
            # Security just pollutes the namespace, and it is unlikely
            # a user would want to search for this themselves
            pass
        elif "properties" in value:
            # Recursive node
            sub_results = _convert_opensearch_mapping_to_flat(value["properties"])
            for sub_key, sub_value in sub_results.items():
                results[key + "." + sub_key] = sub_value
        elif "type" in value:
            # Assume a leaf node
            results[key] = value["type"]
        else:
            raise ValueError("Bad mapping object:" + repr(value))

    return results


@cachetools.cached(cache=memcache.get_ttl_cache("binary2_model"), key=lambda x: x.sd.unique())
def get_opensearch_binary_mapping(ctx: Context) -> dict[str, str]:
    """Returns the model used for binaries."""
    # There is a local copy of the initial indices in binary.map_result,
    # but this doesn't include plugin results
    # Probe OpenSearch instead
    indices = ctx.man.binary2.w.get_indices(ctx.sd)

    # This will be a dictionary of the different indexes available
    # Flatten this
    map_result = {}
    for index in indices.values():
        map_result.update(index["mappings"]["properties"])

    # Transform map_result to a flat dictionary
    flattened_dict = _convert_opensearch_mapping_to_flat(map_result)

    # Add additional pre-filter keys
    flattened_dict[BINARY_TAG_KEY] = "keyword"
    flattened_dict[FEATURE_TAG_KEY] = "keyword"

    return flattened_dict
