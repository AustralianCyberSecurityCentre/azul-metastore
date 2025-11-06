"""Queries for counting features and feature values."""

from __future__ import annotations

import json
import logging
import re

from azul_bedrock import models_restapi
from pydantic import TypeAdapter

from azul_metastore import context
from azul_metastore.common.utils import md5
from azul_metastore.context import Context
from azul_metastore.query import cache
from azul_metastore.query.annotation import add_feature_value_tags_legacy

logger = logging.getLogger(__name__)
readFeatureValuesValue_list_adapter = TypeAdapter(list[models_restapi.ReadFeatureValuesValue])


def feature_count_tags(ctx: context.Context, features: list[str]) -> list[dict]:
    """Count value tags."""
    if not features:
        return []
    body = {
        "query": {"bool": {"filter": [{"term": {"type": "fv_tag"}}, {"terms": {"name": features}}]}},
        "aggs": {"name": {"terms": {"field": "name", "size": 8000}}},
        "size": 0,
    }
    resp = ctx.man.annotation.w.search(ctx.sd, body=body)
    return [dict(name=x["key"], tags=x["doc_count"]) for x in resp["aggregations"]["name"]["buckets"]]


def count_values_in_features(
    ctx: context.Context, features: list[str], *, skip_count=False, filters: list[dict] = None
) -> list[models_restapi.FeatureMulticountRet]:
    """Count features."""
    category = "feature.values"
    if not filters:
        filters = []
    uniq = md5(json.dumps(filters))
    # read from cache
    counted = cache.load_counts(ctx, category, uniq, features)
    # list of things not in the cache that we need to calculate manually
    list_missing = list(set(features).difference(counted.keys()))
    # count things not in cache
    from_calc = {}
    if list_missing and not skip_count:
        body = {
            "timeout": "5000ms",
            "size": 0,
            "query": {
                "bool": {
                    "filter": filters,
                    "should": [{"exists": {"field": f"features_map.{x}"}} for x in list_missing],
                    "minimum_should_match": 1,
                }
            },
            "aggs": {x: {"cardinality": {"field": f"features_map.{x}"}} for x in list_missing},
        }
        resp = ctx.man.binary2.w.search(ctx.sd, body=body)
        for key, row in resp["aggregations"].items():
            from_calc[key] = row["value"]
        # save to cache
        cache.store_counts(ctx, category, uniq, from_calc)

    counted.update(from_calc)
    return [models_restapi.FeatureMulticountRet(name=x, values=y) for x, y in counted.items()]


def count_binaries_with_feature_names(
    ctx: context.Context, features: list[str], *, skip_count=False, filters: list[dict] = None
) -> list[models_restapi.FeatureMulticountRet]:
    """Count features."""
    category = "feature.entities"
    if not filters:
        filters = []
    uniq = md5(json.dumps(filters))
    # read from cache
    counted = cache.load_counts(ctx, category, uniq, features)
    # list of things not in the cache that we need to calculate manually
    list_missing = list(set(features).difference(counted.keys()))
    # count things not in cache
    from_calc = {}
    if list_missing and not skip_count:
        body = {
            "timeout": "5000ms",
            "size": 0,
            "query": {
                "bool": {
                    "filter": filters,
                    "should": [{"exists": {"field": f"features_map.{x}"}} for x in list_missing],
                    "minimum_should_match": 1,
                }
            },
            "aggs": {
                "features": {
                    "filters": {"filters": {x: {"exists": {"field": f"features_map.{x}"}} for x in list_missing}},
                    "aggs": {"num_entities": {"cardinality": {"field": "sha256", "precision_threshold": 100}}},
                }
            },
        }
        resp = ctx.man.binary2.w.search(ctx.sd, body=body)
        for k, row in resp["aggregations"]["features"]["buckets"].items():
            from_calc[k] = row["num_entities"]["value"]
        # save to cache
        cache.store_counts(ctx, category, uniq, from_calc)

    counted.update(from_calc)
    return [models_restapi.FeatureMulticountRet(name=x, entities=y) for x, y in counted.items()]


def count_binaries_with_feature_values(
    ctx: context.Context,
    features: list[models_restapi.ValueCountItem],
    *,
    skip_count=False,
    filters: list[dict] = None,
) -> list[models_restapi.ValueCountRet]:
    """Return counts for all feature values required."""
    category = "feature_value.entities"
    if not filters:
        filters = []
    uniq = md5(json.dumps(filters))
    # use md5 since value can be long
    feature_values = {
        f"{x.name}.{md5(x.value)}": models_restapi.ValueCountRet(entities=-1, **x.model_dump()) for x in features
    }
    # read from cache
    counted = cache.load_counts(ctx, category, uniq, feature_values.keys())
    # list of things not in the cache that we need to calculate manually
    missing_ids = list(set(feature_values.keys()).difference(counted.keys()))
    missing_items = {x: y for x, y in feature_values.items() if x in missing_ids}
    # count things not in cache
    from_calc = {}
    if missing_items and not skip_count:
        nvs = [(y.name, y.value, x) for x, y in missing_items.items()]
        asmap = {}
        for name, value, _ in nvs:
            asmap.setdefault(name, set()).add(value)
        body = {
            "timeout": "5000ms",
            "size": 0,
            "query": {
                "bool": {
                    "filter": filters,
                    "should": [{"terms": {f"features_map.{name}": sorted(values)}} for name, values in asmap.items()],
                    "minimum_should_match": 1,
                }
            },
            "aggs": {
                "fv": {
                    "filters": {"filters": {z: {"term": {f"features_map.{name}": value}} for name, value, z in nvs}},
                    "aggs": {"num_entities": {"cardinality": {"field": "sha256", "precision_threshold": 100}}},
                }
            },
        }
        resp = ctx.man.binary2.w.search(ctx.sd, body=body)
        for k, row in resp["aggregations"]["fv"]["buckets"].items():
            from_calc[k] = row["num_entities"]["value"]
        # save to cache
        cache.store_counts(ctx, category, uniq, from_calc)

    counted.update(from_calc)
    for id, count in counted.items():
        feature_values[id].entities = count

    # filter out unavailable entries (such as if skip_count=true)
    return [x for x in feature_values.values() if x.entities >= 0]


def count_binaries_with_part_values(
    ctx: context.Context,
    parts: list[models_restapi.ValuePartCountItem],
    *,
    skip_count=False,
    filters: list[dict] = None,
) -> list[models_restapi.ValuePartCountRet]:
    """Return counts for all feature parts required."""
    category = "feature_derived.entities"
    if not filters:
        filters = []
    uniq = md5(json.dumps(filters))
    # use md5 since value can be long
    featureparts = {
        f"{x.part}.{md5(x.value)}": models_restapi.ValuePartCountRet(entities=-1, **x.model_dump()) for x in parts
    }
    # read from cache
    counted = cache.load_counts(ctx, category, uniq, featureparts.keys())
    # list of things not in the cache that we need to calculate manually
    missing_ids = list(set(featureparts.keys()).difference(counted.keys()))
    missing_items = {x: y for x, y in featureparts.items() if x in missing_ids}
    # count things not in cache
    from_calc = {}
    if missing_items and not skip_count:
        mapr = {
            "filepath_unix": "filepath.tree",
            "filepath_unixr": "filepath.tree_reversed",
        }
        nvs = [(mapr.get(y.part, y.part), y.value, x) for x, y in missing_items.items()]
        body = {
            "timeout": "5000ms",
            "size": 0,
            "query": {
                "bool": {
                    "filter": filters,
                    "should": [{"term": {f"features.enriched.{x}": y}} for x, y, z in nvs],
                    "minimum_should_match": 1,
                },
            },
            "aggs": {
                "fv": {
                    "filters": {"filters": {z: {"term": {f"features.enriched.{x}": y}} for x, y, z in nvs}},
                    "aggs": {"num_entities": {"cardinality": {"field": "sha256", "precision_threshold": 100}}},
                }
            },
        }
        resp = ctx.man.binary2.w.search(ctx.sd, body=body)
        for k, row in resp["aggregations"]["fv"]["buckets"].items():
            from_calc[k] = row["num_entities"]["value"]
        # save to cache
        cache.store_counts(ctx, category, uniq, from_calc)

    counted.update(from_calc)
    for id, count in counted.items():
        featureparts[id].entities = count

    # filter out unavailable entries (such as if skip_count=true)
    return [x for x in featureparts.values() if x.entities >= 0]


def find_feature_values(
    ctx: Context,
    feature: str,
    *,
    num_values: int = 500,
    term: str = "",
    sort_asc: bool = True,
    case_insensitive: bool = False,
    filters: list[dict] = None,
    after: str | None = None,
) -> models_restapi.ReadFeatureValues:
    """Search for a feature's value matching the provided term.

    This function supports recalling every feature matching the criteria.

    The composite pagination provided by opensearch is not 'point-in-time' so additional docs added after pagination
    starts may be present in the results.

    :param ctx: query context object
    :param feature: the name of the  feature to search for values for.

    :param num_values: number of values to return per request.
    :param term: An free text search in Azul's search syntax
    :param after: json encoded after list (to prevent network encoding issues)

    :return: dictionary with number of results and limited list of results
    """
    # FUTURE could use PIT point-in-time to stabilise these results.
    #  Right now, if new docs enter the system after the first search, they may
    #  be included in results depending on timing.
    if not filters:
        filters = []

    sorter = "asc" if sort_asc else "desc"

    logger.debug(f"find_all_feature_values {feature=} {term=} {sorter=} {case_insensitive=} {filters=}")
    # setup return values.
    total = None
    after_key_value = ""
    retvals = []
    total_values_filtered_by_term = 0
    ftype = None
    retvals_final: list[models_restapi.ReadFeatureValuesValue] = []
    while len(retvals_final) < num_values and after_key_value is not None:
        body = {
            "size": 1,
            "_source": ["features"],
            "query": {
                "bool": {
                    "filter": [
                        (
                            {
                                "regexp": {
                                    f"features_map.{feature}": {
                                        "value": rf".*{re.escape(term)}.*",
                                        "case_insensitive": case_insensitive,
                                    }
                                }
                            }
                            if term
                            else {"exists": {"field": f"features_map.{feature}"}}
                        ),
                    ]
                    + filters,
                }
            },
            "aggs": {
                "COMPOSITE": {
                    "composite": {
                        "size": num_values,
                        "sources": [{"VALUE": {"terms": {"field": f"features_map.{feature}", "order": sorter}}}],
                    },
                    "aggs": {"max_timestamp": {"max": {"field": "timestamp"}}},
                }
            },
        }

        if after:
            # resume pagination of existing search
            body["aggs"]["COMPOSITE"]["composite"]["after"] = json.loads(after)
        else:
            # first request so count expected number of records
            body["aggs"]["TOTAL"] = {"cardinality": {"field": f"features_map.{feature}", "precision_threshold": 1000}}

        resp = ctx.man.binary2.w.search(ctx.sd, body=body)

        after_key_value = resp["aggregations"]["COMPOSITE"].get("after_key", None)
        after = json.dumps(after_key_value) if after_key_value else None

        if "TOTAL" in resp["aggregations"]:
            total = resp["aggregations"]["TOTAL"]["value"]

        retvals = [
            models_restapi.ReadFeatureValuesValue(
                name=feature,
                value=x["key"].get("VALUE", None),
                newest_processed=x["max_timestamp"]["value_as_string"],
            )
            for x in resp["aggregations"]["COMPOSITE"]["buckets"]
        ]

        # get feature data type
        ftype = None
        if resp.get("hits", {}).get("hits"):
            for x in resp["hits"]["hits"][0].get("_source", {}).get("features", []):
                if x.get("name") == feature:
                    ftype = x["type"]
        # filter values by search term
        if term:
            reg = re.compile(term, re.IGNORECASE if case_insensitive else 0)
            for retval in retvals:
                # run regex manually
                if reg.search(retval.value):
                    retval.score = 1
                else:
                    retval.score = 0
                    total_values_filtered_by_term += 1
            retvals = [x for x in retvals if x.score > 0]

        if len(retvals_final) + len(retvals) > num_values:
            values_to_add = num_values - len(retvals_final)
            retvals = retvals[:values_to_add]
            after = json.dumps({"VALUE": retvals[-1].value})

        retvals_final = retvals_final + retvals

    # FUTURE should this be separate?
    # load tags
    add_feature_value_tags_legacy(ctx, retvals_final)

    ret_val = models_restapi.ReadFeatureValues(
        name=feature,
        values=retvals_final,
    )
    if ftype:
        ret_val.type = ftype

    ret_val.is_search_complete = len(retvals_final) < num_values

    # assemble final result object and avoid setting properties if they are None
    if after:
        ret_val.after = after
    if total:
        ret_val.total = total
        # If it's a term query approximate the total unless all values are already in the response.
        if ret_val.is_search_complete:
            ret_val.total = len(ret_val.values)
        elif term:
            ret_val.is_total_approx = True
            # Approximate number of good values by assuming all queries will return a similar number of values.
            # And then multiplying that by the original total knowing that some will be filtered.
            fraction_of_good_values = len(ret_val.values) / (total_values_filtered_by_term + len(ret_val.values))
            ret_val.total = int(total * fraction_of_good_values)
    return ret_val
