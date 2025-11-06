"""Trivial system queries to verify simple things."""

import json

from azul_metastore.context import Context


def find_binaries_with_separated_features(ctx: Context, f1: str, f2: str):
    body = {
        "size": 100,
        "query": {
            "bool": {
                "filter": [
                    {
                        "has_child": {
                            "type": "metadata",
                            "query": {"bool": {"filter": [{"exists": {"field": f"features_map.{f1}"}}]}},
                        }
                    },
                    {
                        "has_child": {
                            "type": "metadata",
                            "query": {"bool": {"filter": [{"exists": {"field": f"features_map.{f2}"}}]}},
                        }
                    },
                ]
            }
        },
    }
    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    return [x["_id"] for x in resp["hits"]["hits"]]


def count_total_binary_links(ctx: Context):
    body = {
        "track_total_hits": True,
        "size": 0,
        "query": {"bool": {"filter": [{"exists": {"field": "parent"}}]}},
    }

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    event_count = resp["hits"]["total"]["value"]
    return event_count


def count_total_binary_results(ctx: Context):
    body = {
        "track_total_hits": True,
        "size": 0,
        "query": {"bool": {"filter": [{"exists": {"field": "sha256"}}]}},
    }

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    event_count = resp["hits"]["total"]["value"]
    return event_count


def count_total_binary_submissions(ctx: Context):
    """Count links"""
    body = {
        "track_total_hits": True,
        "size": 0,
        "query": {"bool": {"filter": [{"exists": {"field": "source"}}]}},
    }

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    event_count = resp["hits"]["total"]["value"]
    return event_count


def summarise_parents(ctx: Context):
    """Print parents sha256s and total children of each type."""
    body = {
        "size": 0,
        "query": {
            "bool": {"filter": [{"has_child": {"type": "metadata", "query": {"exists": {"field": "binary_info"}}}}]}
        },
        "aggs": {
            "CHILDREN": {
                "children": {"type": "metadata"},
                "aggs": {
                    "SHA256": {
                        "terms": {"field": "sha256", "size": 1000},
                        "aggs": {
                            "SUBMISSIONS": {"filter": {"exists": {"field": "source"}}},
                            "LINKS": {"filter": {"exists": {"field": "parent"}}},
                            "RESULTS": {
                                "filter": {
                                    "bool": {
                                        "must_not": [
                                            {"exists": {"field": "parent"}},
                                            {"exists": {"field": "source"}},
                                        ]
                                    }
                                }
                            },
                        },
                    },
                },
            }
        },
    }

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    ret = []
    for raw in resp["aggregations"]["CHILDREN"]["SHA256"]["buckets"]:
        submissions = raw["SUBMISSIONS"]["doc_count"]
        links = raw["LINKS"]["doc_count"]
        results = raw["RESULTS"]["doc_count"]
        ret.append(f'{raw["key"]} -> {submissions=} {links=} {results=}')

    print("summarised parents")
    print("\n".join(ret))


def summarise_submissions(ctx: Context):
    """Return text representation of 1000 submissions for debugging."""
    body = {
        "size": 1000,
        "query": {"bool": {"filter": [{"exists": {"field": "source"}}]}},
    }

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    ret = []
    for raw in resp["hits"]["hits"]:
        hit = raw["_source"]
        source = hit["source"]
        child = hit["sha256"]
        ret.append(
            f'{raw["_source"]["track_source_references"]} | source={source["name"]} -> sha256={child} {json.dumps(source["references"])}'
        )

    print("summarised submissions")
    print("\n".join(ret))


def summarise_links(ctx: Context):
    """Return text representation of 1000 links for debugging."""
    body = {
        "size": 1000,
        "query": {"bool": {"filter": [{"exists": {"field": "parent"}}]}},
    }

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    ret = []
    for raw in resp["hits"]["hits"]:
        hit = raw["_source"]
        parent = hit["parent"]["sha256"]
        ret.append(
            f'{raw["_source"]["track_link"]} | {parent} --{hit["action"]}-{hit["author"]["name"]}--> {hit["sha256"]}'
        )

    print("summarised links")
    print("\n".join(ret))


def summarise_results(ctx: Context):
    """Return text representation of 1000 results for debugging."""
    body = {
        "size": 1000,
        "query": {
            "bool": {
                "filter": [{"exists": {"field": "sha256"}}],
            }
        },
    }

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    ret = []
    for raw in resp["hits"]["hits"]:
        hit = raw["_source"]
        author = hit["author"]
        ret.append(
            f'{raw["_source"]["track_author"]} | {raw["_source"]['sha256']}-{hit["action"]} from {author["name"]}:{author["version"]}'
        )

    print("summarised results")
    print("\n".join(ret))


def summarise(ctx: Context):
    summarise_parents(ctx)
    summarise_submissions(ctx)
    summarise_links(ctx)
    summarise_results(ctx)
