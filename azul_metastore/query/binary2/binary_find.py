"""Queries for finding binaries."""

import itertools
import logging
from typing import Optional

from azul_bedrock.models_restapi import binaries as bedr_binaries
from azul_bedrock.models_restapi.binaries_auto_complete import AutocompleteContext
from fastapi import HTTPException
from lark import UnexpectedInput

from azul_metastore.common import search_query, wrapper
from azul_metastore.common.search_query import QueryExtraInfo, az_query_to_opensearch
from azul_metastore.common.search_query_parser import parse
from azul_metastore.context import Context
from azul_metastore.query.annotation import read_binaries_tags

logger = logging.getLogger(__name__)


def _summarise_hashes(
    ctx: Context,
    binaries: dict[str, dict],
) -> None:
    """Retrieve summary information for specific binaries."""
    # begin constructing main search object
    sha256s = [x for x in binaries.keys()]
    # for deduplicating docs from each source.name
    # Sorting by filename, mime, magic to ensure they are present in the document.
    # sort by entity datastreams label if present, to proritise source info that has backing data
    # then sort by depth to get least deep doc
    # then sort by timestamp to get newest doc
    body = {
        "query": {"bool": {"filter": [{"terms": {"sha256": sha256s}}]}},
        "size": 0,
        "_source": False,
        "aggs": {
            "SHA256": {
                "terms": {"field": "sha256", "size": len(sha256s)},
                "aggs": {
                    "SOURCE": {
                        "terms": {"field": "source.name", "size": 3},
                        "aggs": {
                            "SUMMARY": {
                                "top_hits": {
                                    "size": 1,
                                    "_source": [
                                        "depth",
                                        "source.name",
                                        "source.references",
                                        "source.timestamp",
                                        "file_format_legacy",
                                        "file_format",
                                        "file_extension",
                                        "size",
                                        "magic",
                                        "mime",
                                        "md5",
                                        "sha1",
                                        "sha256",
                                        "sha512",
                                        "tlsh",
                                        "ssdeep",
                                        "datastreams.label",
                                        "filename",
                                    ],
                                    "sort": [
                                        {"filename": "desc"},
                                        {"file_format": "desc"},
                                        {"mime": "desc"},
                                        {"magic": "desc"},
                                        {"datastreams.label": "asc"},
                                        {"depth": "asc"},
                                        {"source.timestamp": "desc"},
                                    ],
                                }
                            }
                        },
                    }
                },
            }
        },
    }
    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    for bsha256 in resp["aggregations"]["SHA256"]["buckets"]:
        sha256 = bsha256["key"]
        summaries = []
        for bsource in bsha256["SOURCE"]["buckets"]:
            hit = bsource["SUMMARY"]["hits"]["hits"][0]
            src = hit["_source"]
            summaries.append(
                {
                    "file_format_legacy": src.get("file_format_legacy"),
                    "file_format": src.get("file_format"),
                    "file_extension": src.get("file_extension"),
                    "file_size": src.get("size"),
                    "md5": src.get("md5"),
                    "sha1": src.get("sha1"),
                    "sha256": src.get("sha256"),
                    "sha512": src.get("sha512"),
                    "ssdeep": src.get("ssdeep"),
                    "tlsh": src.get("tlsh"),
                    "magic": src.get("magic"),
                    "mime": src.get("mime"),
                    "filenames": src.get("filename", []),
                    "exists": bool(src.get("source", {})),
                    "has_content": any(y["label"] == "content" for y in src.get("datastreams", [])),
                    "sources": {
                        "depth": src.get("depth"),
                        "name": src.get("source", {}).get("name"),
                        "references": src.get("source", {}).get("references", {}),
                        "timestamp": src.get("source", {}).get("timestamp"),
                    },
                }
            )

        # combine summaries
        def _all(vals):
            """Return all truthy values or None."""
            tmp = [x for x in vals if x]
            return tmp if tmp else None

        def _any(vals):
            """Return first truthy value or None."""
            tmp = [x for x in vals if x]
            return tmp[0] if tmp else None

        ret = {
            "exists": any([x["exists"] for x in summaries]),
            "has_content": any([x["has_content"] for x in summaries]),
            "sha256": sha256,
            "file_format_legacy": _any(x["file_format_legacy"] for x in summaries),
            "file_format": _any(x["file_format"] for x in summaries),
            "file_extension": _any(x["file_extension"] for x in summaries),
            "file_size": _any(x["file_size"] for x in summaries),
            "md5": _any(x["md5"] for x in summaries),
            "sha1": _any(x["sha1"] for x in summaries),
            "sha512": _any(x["sha512"] for x in summaries),
            "ssdeep": _any(x["ssdeep"] for x in summaries),
            "tlsh": _any(x["tlsh"] for x in summaries),
            "magic": _any(x["magic"] for x in summaries),
            "mime": _any(x["mime"] for x in summaries),
            "filenames": _all(x for x in itertools.chain.from_iterable([x["filenames"] for x in summaries])),
            "sources": [x["sources"] for x in summaries if x["sources"]["name"]],
        }

        # sort sources by id and timestamp
        srcmap = {}
        for src in ret["sources"]:
            srcmap[src["name"] + src["timestamp"]] = src
        ret["sources"] = sorted(srcmap.values(), key=lambda x: (x["name"], x["timestamp"]))

        # remove null and empty lists
        for k in list(ret.keys()):
            if ret[k] is None or (isinstance(ret[k], list) and not len(ret[k])):
                ret.pop(k)

        binaries[sha256].update(ret)

    # for any binaries with no queryable records, record that we have no data for them
    for k in sha256s:
        if not binaries[k]:
            binaries[k] = {"exists": False, "has_content": False}


def _wrap_search_has_child(query: list[dict], include_highlights: bool) -> dict:
    """Wraps a query in appropriate has_child query with highlighting."""
    counter = 0

    def _wrap_inner(inner):
        nonlocal counter
        counter += 1
        return_val = {
            "has_child": {
                "type": "metadata",
                "query": inner,
            }
        }
        if include_highlights:
            # has_child highlighting can only be retrieved via inner hits
            return_val["has_child"]["inner_hits"] = {
                "_source": False,
                "name": f"{counter}",  # must use unique names
                "highlight": {
                    "order": "score",
                    "fields": {"*": {}},
                    "pre_tags": [""],
                    "post_tags": [""],
                    "encoder": "default",
                },
            }
        return return_val

    ret = []
    for row in query:
        filters = row.get("bool", {}).get("filter", [])
        has_must_not = row.get("bool", {}).get("must_not", [])
        # If a filter is next to a must_not leave the two within the same has_child so that child query must
        # match the filter.
        if filters and not has_must_not:
            # Ensure that 'filter' matches on any metadata doc rather than
            # requiring all filters to match on 1 metdata doc.
            # Not applied to 'should' or 'must_not' as it doesn't impact their outcome.
            for i in range(len(filters)):
                filters[i] = _wrap_inner(filters[i])
            ret.append(row)
        else:
            # Ensure query runs on child metadata doc
            ret.append(_wrap_inner(row))
    return ret


def find_binaries(
    ctx: Context,
    *,
    sort: bedr_binaries.FindBinariesSortEnum = bedr_binaries.FindBinariesSortEnum.score,
    sort_asc: bool = False,
    term: Optional[str] = None,
    hashes: Optional[list[str]] = None,
    max_binaries: int = 100,
    count_binaries: bool = False,
) -> bedr_binaries.EntityFind:
    """Search for entities matching specific criteria.

    Due to structure of documents (1 doc per path per plugin per entity), its not possible to search across
    the results of multiple plugins. i.e. entity WHERE feature 1 FROM plugin 1 AND feature 2 FROM plugin 2
    Thus, all features, values and feature_values must be produced by a single author for this to work as expected.

    :param ctx: query context object
    :param sort: order of entities in results
    :param sort_asc: sort in ascending order if true
    :param term: An free text search in Azul's search syntax
    :param hashes: list of binary hashes to search
    :param max_binaries: limit resulting entities to this number
    :param count_binaries: will count the total number of matching binaries if true
    :return: dictionary with number of results and limited list of results
    """
    if max_binaries > 1000:
        raise wrapper.InvalidSearchException("max entities too large")
    if not hashes:
        hashes = []

    # create custom sort parameters from input
    asc_or_desc = "asc" if sort_asc else "desc"

    # begin constructing main search object
    body = {
        "query": {
            "bool": {
                # ensure we can access a child doc with submission info
                "filter": [{"has_child": {"type": "metadata", "query": {"exists": {"field": "source.name"}}}}],
                "should": [],
                # Sorting based on child document values.
            }
        },
        "sort": [{"_score": {"order": asc_or_desc}}],
        "size": max_binaries,
        "_source": False,
        # "collapse": {"field": "sha256"},
        "track_total_hits": True if count_binaries else False,
    }

    # Sorting on child events of a parent binary if requested
    # Need to sort by child events as the parent event is just a sha256.
    if (
        sort == bedr_binaries.FindBinariesSortEnum.source_timestamp
        or sort == bedr_binaries.FindBinariesSortEnum.timestamp
    ):
        body["query"]["bool"]["must"] = {
            "has_child": {
                "type": "metadata",
                "query": {
                    "function_score": {
                        "script_score": {
                            # Painless script that converts the current documents datetime field (or 0 if it's unset)
                            # (probably timestamp or source.timestamp) and converts it to a numeric value for scoring.
                            # This allows sorting by the child date fields via score sorting.
                            "script": f"if(doc['{sort}'].size() != 0){{doc['{sort}'].value.toInstant().toEpochMilli()}} else{{0}}"  # noqa: E501
                        }
                    }
                },
                "score_mode": "max",
            }
        }

    # filters that should also be used to highlight results (added at end of query building)
    qf_highlight = []
    extra_info = QueryExtraInfo()
    if term:
        # Transform an Azul free-text search expression to an OpenSearch query
        try:
            parse_ast = parse(term)
        except UnexpectedInput as e:
            raise HTTPException(status_code=400, detail="Failed to parse term: " + str(e))

        if parse_ast is not None:
            result, extra_info = az_query_to_opensearch(ctx, parse_ast)
            qf_highlight.append(result)

    # FUTURE if all supplied hashes are sha256s, skip to the summarise query
    # SSDeep hashes are case-sensitive
    hashes_normal = []
    for x in hashes:
        if x.count(":") == 2:
            hashes_normal.append(x)
        else:
            hashes_normal.append(x.lower())
    if hashes_normal:
        # similar to term filter, but over less fields and more precise matching
        qf_highlight += [
            {
                "bool": {
                    "should": [
                        {"terms": {"md5": hashes_normal}},
                        {"terms": {"sha1": hashes_normal}},
                        {"terms": {"sha256": hashes_normal}},
                        {"terms": {"sha512": hashes_normal}},
                        {"terms": {"ssdeep.hash": hashes_normal}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        ]
        # biggest of max_entities or one collision for each hash being searched
        body["size"] = max(len(hashes_normal) * 2, max_binaries)

    # Avoid including highlights if the query uses any tags.
    # This avoids issues with cross index searching on very large number of tagged items e.g
    # 1000 binaries with the same tag will break the opensearch query if highlighting is included.
    include_highlights = not extra_info.is_binary_tag_search and not extra_info.is_feature_tag_search
    # ensure queries are has_child compatible
    qf_highlight = _wrap_search_has_child(qf_highlight, include_highlights=include_highlights)
    # add highlight filters to main filter
    body["query"]["bool"]["filter"] += qf_highlight

    # perform search to retrieve matching sha256s
    resp = ctx.man.binary2.w.complex_search(ctx.sd, body=body)

    # store data for entities in same order as found
    binary_info = {}
    for outer in resp["hits"]["hits"]:
        eid = outer["_id"]
        binary_info[eid] = {}

        # collect highlighting information from has_child inner hits
        hls = binary_info[eid].setdefault("highlight", {})
        for x in outer.get("inner_hits", {}).values():
            for y in x["hits"]["hits"]:
                # highlighting may not be present for the hit
                highlight = y.get("highlight")
                if not highlight:
                    continue
                for k, v in highlight.items():
                    if k == "sha256" and v[0] in hashes_normal:
                        # searched for a specific sha256 so don't highlight it
                        continue
                    tmp = hls.setdefault(k, [])
                    tmp += v
                    # deduplicate unique highlights
                    hls[k] = list(set(hls[k]))

    # perform query to enrich with summary info for binary
    if binary_info:
        _summarise_hashes(ctx, binary_info)

    # remove entities that couldn't be summarised
    # the has_child query doesn't filter with user-supplied 'security_exclude' so this is necessary
    binary_info = {x: y for (x, y) in binary_info.items() if "exists" in y}

    # add binary tags to results
    tags = read_binaries_tags(ctx, list(binary_info.keys()))
    for k, v in tags.items():
        if not v:
            continue
        binary_info[k]["tags"] = v

    found_binaries = []
    if hashes_normal:
        # we want to return results in the same order that hashes were supplied
        hashes_info = {}
        for x in hashes_normal:
            # Account for SSDeep hashes
            if x.count(":") == 2:
                hashes_info[x] = []
            else:
                hashes_info[x.lower()] = []

        # track each entity matching each hash
        # there can be multiple matches for any hash due to bad implementations or coincidence
        for tmp in binary_info.values():
            found = False
            # in preferred order, find all given hashes
            for k in ["sha256", "md5", "sha512", "sha1", "ssdeep"]:
                if tmp.get(k) in hashes_info:
                    if found:
                        hashes_info[tmp[k]].append(
                            {
                                "exists": True,
                                "has_content": tmp["has_content"],
                                "is_duplicate_find": True,
                                "key": k,
                            }
                        )
                    else:
                        hashes_info[tmp[k]].append(tmp)
                        found = True

        # record found entities based on hash ordering
        for key in hashes_normal:
            binaries = hashes_info[key]
            if not binaries:
                # default entity - included so CLI/RestAPI users can confirm their binaries were not found.
                binaries.append({"exists": False, "has_content": False})
            for entity in binaries:
                # keep non-lowercased key in response
                entity["key"] = key
            found_binaries += binaries
    else:
        # ordinary search results
        # set key to be binary id
        for k, tmp in binary_info.items():
            tmp["key"] = k
        # dicts in 3.7+ are sorted so this works
        found_binaries = list(binary_info.values())[:max_binaries]

    # assemble final result object
    ret = {"items": found_binaries}
    if count_binaries:
        ret["items_count"] = resp["hits"]["total"]["value"]
    return bedr_binaries.EntityFind(**ret)


def generate_autocomplete(input: str, offset: int) -> AutocompleteContext:
    """Determines what should be autocompleted based on the current user input state."""
    return search_query.generate_autocomplete(input, offset)
