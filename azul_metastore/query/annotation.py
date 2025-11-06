"""Queries for reading/writing annotations like comments and tags."""

from __future__ import annotations

from azul_bedrock import models_restapi

from azul_metastore.context import Context
from azul_metastore.encoders import annotation


def read_binaries_tags(ctx: Context, eids: list[str]):
    """Return tags for the supplied entities."""
    eids = [x.lower() for x in eids]
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": "entity_tag"}},
                    {"terms": {"sha256": eids}},
                ],
                "must_not": [{"term": {"state": "disabled"}}],
            }
        },
        "sort": ["tag"],
        "size": 100,
    }
    resp = ctx.man.annotation.w.search(ctx.sd, body=body)

    ret = {x: [] for x in eids}
    for x in resp["hits"]["hits"]:
        ann = annotation.Annotation.decode(x["_source"])
        ret[x["_source"]["sha256"]].append(ann)
    return ret


def read_binary_tags(ctx: Context, sha256: str) -> list[models_restapi.EntityTag]:
    """Return tags for the entity."""
    sha256 = sha256.lower()
    # read all tags for current entity + get counts for each tag
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": "entity_tag"}},
                    {"term": {"sha256": sha256}},
                ],
                "must_not": [{"term": {"state": "disabled"}}],
            }
        },
        "sort": ["tag"],
        "size": 100,
    }
    resp = ctx.man.annotation.w.search(ctx.sd, body=body)
    tags = {x["_source"]["tag"]: x["_source"] for x in resp["hits"]["hits"]}

    # count other entities with same tag
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": "entity_tag"}},
                    {"terms": {"tag": list(tags.keys())}},
                ]
            }
        },
        "aggs": {
            "tags": {
                "terms": {"field": "tag", "size": 100},
                "aggs": {"num_entities": {"cardinality": {"field": "pivot", "precision_threshold": 100}}},
            }
        },
        "size": 0,
    }
    resp = ctx.man.annotation.w.search(ctx.sd, body=body)

    for row in resp["aggregations"]["tags"]["buckets"]:
        tags[row["key"]]["num_entities"] = row["num_entities"]["value"]

    tags = list(tags.values())
    tags = [models_restapi.EntityTag(**ctx.man.annotation.decode(x)) for x in tags]

    return tags


def read_all_binary_tags(ctx: Context) -> models_restapi.ReadTags:
    """Read all entity tags in the system."""
    ret = models_restapi.ReadTags(tags=[], num_tags=0, num_tags_approx=False)
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": "entity_tag"}},
                ],
                "must_not": [{"term": {"state": "disabled"}}],
            }
        },
        "aggs": {
            "num_tags": {"cardinality": {"field": "tag"}},
            "tags": {
                "terms": {"field": "tag", "size": 1000},
                "aggs": {"num_entities": {"cardinality": {"field": "pivot"}}},
            },
        },
        "size": 0,
    }
    resp = ctx.man.annotation.w.search(ctx.sd, body=body)

    ret.num_tags = resp["aggregations"]["num_tags"]["value"]
    ret.num_tags_approx = True
    for row in resp["aggregations"]["tags"]["buckets"]:
        ret.tags.append(
            models_restapi.ReadTagsTag(
                tag=row["key"], num_entities=row["num_entities"]["value"], num_entities_approx=True
            )
        )
    return ret


def create_binary_tags(ctx: Context, owner: str, tags: list[dict]) -> None:
    """Store entity tag in metastore."""
    for tag in tags:
        tag["owner"] = owner
        tag["type"] = "entity_tag"
    # FUTURE normalise?, lowercase?
    ctx.man.annotation.w.wrap_and_index_docs(ctx.sd, [ctx.man.annotation.encode(x) for x in tags], refresh=True)


def delete_binary_tag(ctx: Context, sha256: str, tag: str) -> models_restapi.AnnotationUpdated:
    """Remove entity tag from metastore."""
    sha256 = sha256.lower()
    resp = ctx.man.annotation.w.update(
        ctx.sd,
        body={
            "script": {"source": "ctx._source.state='disabled'", "lang": "painless"},
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"type": "entity_tag"}},
                        {"term": {"sha256": sha256}},
                        {"term": {"tag": tag}},
                    ]
                }
            },
        },
        refresh=True,
    )
    return models_restapi.AnnotationUpdated(
        total=resp.get("total", 0), updated=resp.get("updated", 0), deleted=resp.get("deleted", 0)
    )


def read_all_feature_value_tags(ctx: Context) -> models_restapi.ReadFeatureValueTags:
    """Read all feature value tags in the system."""
    ret = models_restapi.ReadFeatureValueTags(tags=[], num_tags=0)
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": "fv_tag"}},
                ],
                "must_not": [{"term": {"state": "disabled"}}],
            }
        },
        "aggs": {
            "num_tags": {"cardinality": {"field": "tag"}},
            "tags": {
                "terms": {"field": "tag", "size": 1000},
                "aggs": {"num_feature_values": {"cardinality": {"field": "pivot"}}},
            },
        },
        "size": 0,
    }
    resp = ctx.man.annotation.w.search(ctx.sd, body=body)

    ret.num_tags = resp["aggregations"]["num_tags"]["value"]
    for row in resp["aggregations"]["tags"]["buckets"]:
        ret.tags.append(
            models_restapi.ReadFeatureValueTagsTag(
                tag=row["key"], num_feature_values=row["num_feature_values"]["value"]
            )
        )
    return ret


def read_feature_values_for_tag(ctx: Context, tag: str) -> models_restapi.ReadFeatureTagValues:
    """Return feature values in tag."""
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": "fv_tag"}},
                    {"term": {"tag": tag}},
                ],
                "must_not": [{"term": {"state": "disabled"}}],
            }
        },
        "sort": ["tag"],
        "size": 10000,
    }
    resp = ctx.man.annotation.w.search(ctx.sd, body=body)
    fvs = [x["_source"] for x in resp["hits"]["hits"]]
    fvs = [ctx.man.annotation.decode(x) for x in fvs]
    return models_restapi.ReadFeatureTagValues(**{"items": fvs})


def create_feature_value_tags(ctx: Context, owner: str, tags: list[dict]) -> None:
    """Store feature value tag in metastore."""
    for tag in tags:
        tag["owner"] = owner
        tag["type"] = "fv_tag"
        tag["tag"] = tag["tag"].lower()
    docs = [ctx.man.annotation.encode(x) for x in tags]
    ctx.man.annotation.w.wrap_and_index_docs(ctx.sd, docs, refresh=True)


def delete_feature_value_tag(ctx: Context, feature: str, value: str, tag: str):
    """Remove feature value tag from metastore."""
    ctx.man.annotation.w.update(
        ctx.sd,
        body={
            "script": {"source": "ctx._source.state='disabled'", "lang": "painless"},
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"type": "fv_tag"}},
                        {"term": {"feature_name": feature}},
                        {"term": {"feature_value": value}},
                        {"term": {"tag": tag}},
                    ]
                }
            },
        },
        refresh=True,
    )


def add_feature_value_tags_legacy(ctx, features: list[models_restapi.ReadFeatureValuesValue]):
    """Deprecated. Read value tags for specified features and handle everything as dict."""
    fvs = {f"{x.name}.{x.value}": x for x in features}
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": "fv_tag"}},
                    {"terms": {"pivot": sorted(fvs.keys())}},
                ],
                "must_not": [{"term": {"state": "disabled"}}],
            }
        },
        "aggs": {"fv": {"terms": {"field": "pivot", "size": 10000}, "aggs": {"hit": {"top_hits": {"size": 50}}}}},
        "size": 0,
    }

    resp = ctx.man.annotation.w.search(ctx.sd, body=body)

    for feature_value in resp["aggregations"]["fv"]["buckets"]:
        fvs[feature_value["key"]].tags = [
            ctx.man.annotation.decode(x["_source"]) for x in feature_value["hit"]["hits"]["hits"]
        ]


def add_feature_value_tags(ctx, features: list[models_restapi.BinaryFeatureValue]):
    """Read value tags for specified features."""
    fvs = {f"{x.name}.{x.value}": x for x in features}
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": "fv_tag"}},
                    {"terms": {"pivot": sorted(fvs.keys())}},
                ],
                "must_not": [{"term": {"state": "disabled"}}],
            }
        },
        "aggs": {"fv": {"terms": {"field": "pivot", "size": 10000}, "aggs": {"hit": {"top_hits": {"size": 50}}}}},
        "size": 0,
    }

    resp = ctx.man.annotation.w.search(ctx.sd, body=body)

    for feature_value in resp["aggregations"]["fv"]["buckets"]:
        fvs[feature_value["key"]].tags = [
            models_restapi.FeatureValueTag(**ctx.man.annotation.decode(x["_source"]))
            for x in feature_value["hit"]["hits"]["hits"]
        ]
