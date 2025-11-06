"""Queries for reading source data."""

import re
from datetime import datetime
from typing import Optional

from azul_bedrock import models_settings
from azul_bedrock.models_restapi import sources as bedr_sources

from azul_metastore import settings
from azul_metastore.context import Context

from .. import cache


def read_source_references(
    ctx: Context,
    source: str,
    term: Optional[str] = None,
) -> list[bedr_sources.ReferenceSet]:
    """Return source reference information for the source."""
    body = {
        "query": {
            "bool": {
                "filter": [{"term": {"depth": 0}}, {"term": {"source.name": source}}],
            }
        },
        "aggs": {
            "datas": {
                "terms": {"field": "track_source_references", "size": 100, "order": {"newest": "desc"}},
                "aggs": {
                    "newest": {"max": {"field": "source.timestamp"}},
                    "num_entities": {"cardinality": {"field": "sha256"}},
                    "values": {
                        "top_hits": {
                            "size": 1,
                            "_source": {"includes": ["source.references"]},
                            "sort": {"source.timestamp": {"order": "desc"}},
                        }
                    },
                },
            },
        },
        "size": 0,
    }

    if term:
        body["query"]["bool"]["should"] = [
            {
                "regexp": {
                    "source.encoded_references.value": {"value": f".*{re.escape(term)}.*", "case_insensitive": True}
                }
            }
        ]
        body["query"]["bool"]["minimum_should_match"] = 1

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)

    # parse out detailed source information
    ret = []  # list of unique key combinations with summary info
    aggs = resp["aggregations"]
    for bucket in aggs["datas"]["buckets"]:
        approx = bucket["doc_count"] >= 100
        hit = bucket["values"]["hits"]["hits"][0]["_source"]
        ret.append(
            bedr_sources.ReferenceSet(
                track_source_references=bucket["key"],
                timestamp=bucket["newest"]["value_as_string"],
                num_entities=bucket["num_entities"]["value"],
                num_entities_min=approx,
                values=hit.get("source", {}).get("references", {}),
            )
        )

    return ret


def read_submissions(
    ctx: Context, source: str, *, track_source_references: str = None, submission_timestamp: datetime = None
) -> list[bedr_sources.ReferenceSet]:
    """Return all of a sources submission or a specific submission if a timestamp and tracking id is given."""
    body = {
        "query": {"bool": {"filter": [{"term": {"depth": 0}}, {"term": {"source.name": source}}]}},
        "aggs": {
            "datas": {
                "multi_terms": {
                    "size": 100,
                    "order": {"newest": "desc"},
                    "terms": [{"field": "track_source_references"}, {"field": "source.timestamp"}],
                },
                "aggs": {
                    "newest": {"max": {"field": "source.timestamp"}},
                    "num_entities": {"cardinality": {"field": "sha256"}},
                    "values": {
                        "top_hits": {
                            "size": 1,
                            "_source": {
                                "includes": ["source.references", "source.timestamp", "track_source_references"]
                            },
                            "sort": {"source.timestamp": {"order": "desc"}},
                        }
                    },
                },
            }
        },
        "size": 0,
    }

    if submission_timestamp:
        body["query"]["bool"]["filter"].append({"term": {"source.timestamp": submission_timestamp.isoformat()}})

    if track_source_references:
        body["query"]["bool"]["filter"].append({"term": {"track_source_references": track_source_references}})

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)

    # parse out detailed source information
    ret = []  # list of unique key combinations with summary info
    aggs = resp["aggregations"]
    for bucket in aggs["datas"]["buckets"]:
        approx = bucket["doc_count"] >= 100
        hit = bucket["values"]["hits"]["hits"][0]["_source"]
        ret.append(
            bedr_sources.ReferenceSet(
                track_source_references=hit["track_source_references"],
                timestamp=bucket["newest"]["value_as_string"],
                num_entities=bucket["num_entities"]["value"],
                num_entities_min=approx,
                values=hit["source"].get("references", {}),
            )
        )

    return ret


def read_sources() -> dict[str, models_settings.Source]:
    """Return details about all sources."""
    data = settings.get().sources
    ret = {}
    for k in data.keys():
        ret[k] = data[k].model_dump()
    return ret


def read_source(ctx: Context, source: str) -> dict:
    """Return details about a source."""
    body = {
        "query": {"bool": {"filter": [{"term": {"depth": 0}}, {"term": {"source.name": source}}]}},
        "aggs": {
            "newest": {"max": {"field": "source.timestamp"}},
        },
        "size": 0,
    }

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    if not resp["hits"]["total"]["value"]:
        return dict(
            name=source,
            newest=None,
            num_entities=0,
        )
    newest = resp["aggregations"]["newest"]["value_as_string"]

    category = "source.source_entities"
    counted = cache.load_counts(ctx, category, "", [source])
    from_calc = {}
    if not counted:
        body = {
            "timeout": "5000ms",
            "size": 0,
            "query": {"bool": {"filter": [{"term": {"source.name": source}}, {"term": {"depth": 0}}]}},
            "aggs": {
                "sources": {
                    "filters": {"filters": {source: {"term": {"source.name": source}}}},
                    "aggs": {"num_entities": {"cardinality": {"field": "sha256", "precision_threshold": 100}}},
                },
            },
        }
        resp = ctx.man.binary2.w.search(ctx.sd, body=body)
        for k, row in resp["aggregations"]["sources"]["buckets"].items():
            from_calc[k] = row["num_entities"]["value"]
        # save to cache
        cache.store_counts(ctx, category, "", from_calc)

    counted.update(from_calc)
    # parse out summary information
    return dict(name=source, newest=newest, num_entities=counted[source])
