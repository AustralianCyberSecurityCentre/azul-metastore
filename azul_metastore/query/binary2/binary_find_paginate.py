"""Queries for finding binaries."""

import json
import logging
from typing import Optional

from azul_bedrock import models_restapi
from fastapi import HTTPException
from lark import UnexpectedInput

from azul_metastore.common.search_query import az_query_to_opensearch
from azul_metastore.common.search_query_parser import parse
from azul_metastore.context import Context
from azul_metastore.encoders import binary2

logger = logging.getLogger(__name__)


def find_all_binaries(
    ctx: Context,
    *,
    term: Optional[str] = None,
    after: str | None = None,
    num_binaries: int = 1000,
) -> models_restapi.EntityFindSimple:
    """Search for binaries matching specific criteria.

    This function supports recalling every binary matching the criteria.

    Due to structure of documents (1 doc per path per plugin per entity), its not possible to search across
    the results of multiple plugins. i.e. entity WHERE feature 1 FROM plugin 1 AND feature 2 FROM plugin 2
    Thus, all features, values and feature_values must be produced by a single author for this to work as expected.

    The composite pagination provided by opensearch is not 'point-in-time' so additional docs added after pagination
    starts may be present in the results.

    :param ctx: query context object
    :param term: An free text search in Azul's search syntax
    :param after: json encoded after list (to prevent network encoding issues)
    :param num_binaries: Maximum binary sha256s to return per request

    :return: dictionary with number of results and limited list of results
    """
    # FUTURE could use PIT point-in-time to stabilise these results.
    #  Right now, if new docs enter the system after the first search, they may
    #  be included in results depending on timing.
    body = {
        "query": {
            "bool": {
                "filter": [],
                "should": [],
            }
        },
        "size": 0,
        "aggs": {
            "COMPOSITE": {
                "composite": {
                    "size": num_binaries,
                    "sources": [{"SHA256": {"terms": {"field": "sha256"}}}],
                }
            }
        },
    }
    if after:
        # resume pagination of existing search
        body["aggs"]["COMPOSITE"]["composite"]["after"] = json.loads(after)
    else:
        # first request so count expected number of records
        body["aggs"]["TOTAL"] = {"cardinality": {"field": "sha256", "precision_threshold": 1000}}

    if term is not None:
        # Transform an Azul free-text search expression to an OpenSearch query
        try:
            parse_ast = parse(term)
        except UnexpectedInput as e:
            raise HTTPException(status_code=400, detail="Failed to parse term: " + str(e))

        if parse_ast is not None:
            result, _extra_info = az_query_to_opensearch(ctx, parse_ast)
            body["query"]["bool"]["filter"] = [result]

    # perform search
    resp = ctx.man.binary2.w.paginate_search(ctx.sd, body=body)
    after = resp["aggregations"]["COMPOSITE"].get("after_key", None)
    if after:
        after = json.dumps(after)

    total = None
    if "TOTAL" in resp["aggregations"]:
        total = resp["aggregations"]["TOTAL"]["value"]

    found_binaries = []
    for row in resp["aggregations"]["COMPOSITE"]["buckets"]:
        eid = row["key"]["SHA256"]
        found_binaries.append(models_restapi.EntityFindSimpleItem(sha256=eid))
    # assemble final result object and avoid setting properties if they are None
    ret = models_restapi.EntityFindSimple(items=found_binaries)
    if after:
        ret.after = after
    if total:
        ret.total = total
    return ret


def find_all_family_binaries(
    ctx: Context,
    sha256: str,
    is_parent: bool,
    after: str | None = None,
) -> models_restapi.EntityFindSimpleFamily:
    """Search for binaries matching specific criteria.

    This function supports recalling every binary matching the criteria.

    Due to structure of documents (1 doc per path per plugin per entity), its not possible to search across
    the results of multiple plugins. i.e. entity WHERE feature 1 FROM plugin 1 AND feature 2 FROM plugin 2
    Thus, all features, values and feature_values must be produced by a single author for this to work as expected.

    The composite pagination provided by opensearch is not 'point-in-time' so additional docs added after pagination
    starts may be present in the results.

    :param ctx: query context object
    :param term: An free text search in Azul's search syntax
    :param after: json encoded after list (to prevent network encoding issues)
    :param num_binaries: Maximum binary sha256s to return per request

    :return: dictionary with number of results and limited list of results
    """
    # FUTURE could use PIT point-in-time to stabilise these results.
    #  Right now, if new docs enter the system after the first search, they may
    #  be included in results depending on timing.
    body = None
    if not sha256:
        raise Exception("Sha256 to search for parent or child binaries for was not set and should have been!")
    sha256 = sha256.lower()
    # max number of binaries returned per page
    PAGE_SIZE = 50
    sha256_field = "parent.sha256" if is_parent else "sha256"
    sha256_term = "sha256" if is_parent else "parent.sha256"

    body = {
        "track_total_hits": True,
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"term": {sha256_term: sha256}},
                ]
            }
        },
        "aggs": {
            "FAMILY": {
                "composite": {
                    "size": PAGE_SIZE,
                    "sources": [{"sha256": {"terms": {"field": sha256_field}}}],
                },
                "aggs": {
                    "HITS": {
                        "top_hits": {
                            "size": 1,
                            "_source": {"includes": binary2.fields_link + binary2.fields_recover_source_binary_node},
                        }
                    },
                    "NEWEST": {"max": {"field": "timestamp"}},
                },
            }
        },
    }

    if after:
        # resume pagination of existing search
        body["aggs"]["FAMILY"]["composite"]["after"] = json.loads(after)
    else:
        # first request so count expected number of records
        body["aggs"]["TOTAL"] = {"cardinality": {"field": sha256_field, "precision_threshold": 1000}}

    # perform search
    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    after = resp["aggregations"]["FAMILY"].get("after_key", None)
    if after:
        after = json.dumps(after)

    total = None
    if "TOTAL" in resp["aggregations"]:
        total = resp["aggregations"]["TOTAL"]["value"]

    found_binaries = []
    for row in resp["aggregations"]["FAMILY"]["buckets"]:
        eid = row["key"]["sha256"]
        hit = row["HITS"]["hits"]["hits"][0]["_source"]
        if is_parent:
            parent_info = hit.get("parent", {})
            author_info = parent_info.get("author")
        else:
            author_info = hit.get("author", {})

        author_name = author_info.get("name")
        author_category = author_info.get("category")

        found_binaries.append(
            models_restapi.EntityFindSimpleFamilyItem(
                sha256=eid,
                track_link=hit.get("track_link"),
                author_name=author_name,
                author_category=author_category,
                timestamp=hit.get("timestamp"),
            )
        )

    ret = models_restapi.EntityFindSimpleFamily(items=found_binaries)
    if after:
        ret.after = after
    if total and len(found_binaries) > 0:
        ret.total = total
    return ret
