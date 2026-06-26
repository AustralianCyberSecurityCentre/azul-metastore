"""Queries for reading various information about binaries that doesn't fit elsewhere."""

import contextlib

from azul_bedrock import exceptions_bedrock
from azul_bedrock import models_network as azm
from azul_bedrock.exception_enums import ExceptionCodeEnum
from azul_bedrock.exceptions_bedrock import ApiException, BaseAzulException
from azul_bedrock.models_restapi import binaries as bedr_binaries

from azul_metastore.context import Context


def get_total_binary_count(ctx: Context) -> int:
    """Return number of binaries in the system."""
    resp = ctx.man.binary2.w.search(
        ctx.sd,
        {
            "aggs": {
                "entities": {"cardinality": {"field": "sha256"}},
            },
            "size": 0,
        },
    )
    aggs = resp["aggregations"]
    return aggs["entities"]["value"]


def find_stream_references(ctx: Context, sha256: str, is_check_in_dispatcher=True) -> tuple[bool, str, azm.DataLabel]:
    """Return (True, exemplar source_id, exemplar label) if we have bytes backing the given sha256."""
    results = _find_stream_references(ctx, sha256)
    if len(results) == 0:
        return False, "", azm.DataLabel.TEST

    # Verify the binary is also in dispatcher and if not check the next label
    if is_check_in_dispatcher:
        for r in results:
            # Ignore case where dispatcher doesn't find the binary via the label and check the next label.
            with contextlib.suppress(ApiException):
                if ctx.dispatcher.has_binary(source=r[0], label=r[1], sha256=sha256):
                    return True, r[0], r[1]
        return False, "", azm.DataLabel.TEST

    return True, results[0][0], results[0][1]


def _find_stream_references(ctx: Context, sha256: str) -> list[tuple[str, azm.DataLabel]]:
    """Return (True, exemplar source_id, exemplar label) if we have bytes backing the given sha256."""
    if not sha256:
        raise BaseAzulException(internal=ExceptionCodeEnum.MetastoreSha256NotProvidedForFindingStreamRefs)
    sha256 = sha256.lower()
    # as augmented events have no associated submission we need to perform a parent-child query
    body: dict[
        str, int | dict[str, dict[str, list[dict[str, dict[str, str]]]]] | list[dict[str, dict[str, str]]] | list[str]
    ] = {
        "size": 10,
        "query": {"bool": {"filter": [{"term": {"datastreams.sha256": sha256}}]}},
        "sort": [{"timestamp": {"order": "desc"}}],
        "_source": ["source.name", "datastreams.label", "datastreams.sha256"],
    }
    resp = ctx.man.binary2.w.complex_search(ctx.sd, body=body)
    hits = resp.get("hits", {}).get("hits", [])

    results: set[tuple[str, azm.DataLabel]] = set()
    if len(hits) == 0:
        return []
    for h in hits:
        h_source: dict = h.get("_source")

        source_name: str = h_source.get("source", {}).get("name", "")

        for cur_stream in h_source.get("datastreams", []):
            try:
                stream_sha256: str = cur_stream.get("sha256", "")
                if stream_sha256 == sha256:
                    label = azm.DataLabel(cur_stream["label"])
                    results.add((source_name, label))
            except ValueError:
                attempted_label = str(cur_stream.get("label", ""))
                raise exceptions_bedrock.AzulValueError(
                    internal=ExceptionCodeEnum.MetastoreInvalidDataLabel,
                    parameters={"label": attempted_label},
                ) from None

    return list(results)


def find_stream_metadata(ctx: Context, sha256: str, stream_hash: str) -> tuple[str | None, azm.Datastream | None]:
    """Return exemplar stream metadata and an exemplar source for the specified entity id.

    :param ctx: context
    :param sha256: id of entity to find stream on
    :param stream_hash: sha256 of stream to query on entity
    :return: (Source, Found Stream) or (None, None) if not exists/readable
    """
    sha256 = sha256.lower()
    body = {
        "terminate_after": 1,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"sha256": sha256.lower()}},
                    {"term": {"datastreams.sha256": stream_hash.lower()}},
                ],
            }
        },
        "size": 1,
        "_source": {"includes": ["datastreams", "source.name"]},
    }
    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    if not resp["hits"]["hits"]:
        return None, None

    # filter each stream to find the one needed
    datas = [x for x in resp["hits"]["hits"][0]["_source"]["datastreams"] if x["sha256"] == stream_hash.lower()]
    if not datas:
        return None, None
    source = resp["hits"]["hits"][0]["_source"]["source"]["name"]
    return (source, azm.Datastream(**datas[0]))


def check_binaries(ctx: Context, sha256s: list[str]) -> list[dict]:
    """Check each entity to see if they exist."""
    sha256s = [x.lower() for x in sha256s]
    searches = []
    for sha256 in sha256s:
        searches.append({"index": ctx.man.binary2.w.alias})
        searches.append(
            {
                "size": 0,
                "terminate_after": 1,
                "query": {"bool": {"filter": [{"term": {"sha256": sha256}}]}},
            }
        )
    ret = []
    allresp = ctx.man.binary2.w.msearch(ctx.sd, searches=searches)
    for sha256, resp in zip(sha256s, allresp["responses"], strict=False):
        exists = resp["hits"]["total"]["value"] > 0
        ret.append({"sha256": sha256, "exists": exists})
    return ret


def list_all_sources_for_binary(ctx: Context, sha256: str) -> list[str]:
    """Return a list of all sources associated with the entity."""
    sha256 = sha256.lower()
    body = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"sha256": sha256}},
                ]
            }
        },
        "aggs": {"sources": {"terms": {"field": "source.name", "size": 1000}}},
    }

    resp = ctx.man.binary2.w.search(ctx.sd, body=body, routing=sha256)
    sources = []
    for x in resp["aggregations"]["sources"]["buckets"]:
        sources.append(x["key"])
    return sorted(sources)


def get_author_stats(ctx: Context, name: str, version: str) -> int:
    """Return number of unique binaries that exist for a plugin/author."""
    body = {
        "size": 0,
        "query": {"bool": {"filter": [{"term": {"author.name": name}}, {"term": {"author.version": version}}]}},
        "aggs": {
            "entities": {"cardinality": {"field": "sha256"}},
        },
    }
    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    aggs = resp["aggregations"]
    return aggs["entities"]["value"]


def get_binary_newer(ctx: Context, sha256: str, timestamp: str) -> bedr_binaries.BinaryDocuments:
    """Return true if there is data available for the entity newer than the supplied timestamp."""
    sha256 = sha256.lower()
    # this query must match the equivalent part of read()
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"sha256": sha256}},
                    {"range": {"timestamp": {"gt": timestamp}}},
                ]
            }
        },
        "size": 0,
        "aggs": {"NEWEST": {"max": {"field": "timestamp"}}},
    }
    resp = ctx.man.binary2.w.search(ctx.sd, body=body, routing=sha256)
    ret = bedr_binaries.BinaryDocuments(count=0, newest=None)
    if resp["aggregations"]["NEWEST"]["value"]:
        ret = bedr_binaries.BinaryDocuments(
            count=resp["hits"]["total"]["value"], newest=resp["aggregations"]["NEWEST"]["value_as_string"]
        )
    return ret
