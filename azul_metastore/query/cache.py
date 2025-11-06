"""Cache data controllers."""

# FUTURE consider redis for key value store instead

from __future__ import annotations

import logging

import pendulum
from opensearchpy import exceptions as osex

from azul_metastore import context
from azul_metastore.encoders import cache as cc

logger = logging.getLogger(__name__)


def invalidate_all(ctx: context.Context):
    """Invalidate all cache."""
    body = {"query": {"bool": {"filter": [{"match_all": {}}]}}}
    return ctx.man.cache.w.delete(ctx.sd, body=body)


def store_generic(ctx: context.Context, category: str, unique: str, version: str, data: dict, timestamp=None):
    """Store cached dictionary for the id, divided by category."""
    if not timestamp:
        timestamp = pendulum.now(tz=pendulum.UTC).to_iso8601_string()
    doc = {
        "security": ctx.get_user_current_security(),
        "timestamp": timestamp,
        "type": category,
        "unique": unique,
        "version": version,
        "user_security": ctx.get_user_security_unique(),
        "data": data,
    }
    try:
        priv = context.get_writer_context()
    except (context.NoWriteException, osex.AuthenticationException) as e:
        logger.error(f"store_generic error: {str(e)}")
        return
    priv.man.cache.w.wrap_and_index_docs(priv.sd, [cc.Cache.encode(doc)])


def load_generic(
    ctx: context.Context,
    category: str,
    unique: str,
    version: str,
    timestamp: str = None,
) -> dict | None:
    """Load cached dictionary for the id, divided by category."""
    user_security = ctx.get_user_security_unique()
    _id = ctx.man.cache.calc_id(category, unique, user_security)
    # use the priviliged reader, since otherwise the doc needs to have been indexed
    try:
        priv = context.get_writer_context()
    except (context.NoWriteException, osex.AuthenticationException) as e:
        logger.error(f"load_generic error: {str(e)}")
        return None
    resp = ctx.man.cache.w.get(priv.sd, _id)
    if (
        resp
        and resp["version"] == version
        and (not timestamp or pendulum.parse(resp["timestamp"]) >= pendulum.parse(timestamp))
    ):
        return resp["data"]
    return None


def _calc_uniq(val: str, uniq: str):
    return ".".join([val, uniq])


def store_counts(ctx: context.Context, category: str, uniq: str, counts: dict[str, int]) -> None:
    """Store cached counts for list of ids, divided by category.

    The uniq string is added to each id during a query, to capture custom query args being used.
    """
    user_security = ctx.get_user_security_unique()
    uniqued = {_calc_uniq(x, uniq): y for x, y in counts.items()}
    # check all mapper keys to see if need a recount
    base = {
        "timestamp": pendulum.now(tz=pendulum.UTC).to_iso8601_string(),
        "type": category,
        "user_security": user_security,
        "security": ctx.get_user_current_security(),
    }
    docs = [{**base, "unique": x, "count": y} for x, y in uniqued.items()]
    # cache counts for future use
    try:
        priv = context.get_writer_context()
    except (context.NoWriteException, osex.AuthenticationException) as e:
        logger.error(f"store_counts error: {str(e)}")
        return
    encoded = [cc.Cache.encode(x) for x in docs]
    errors = priv.man.cache.w.wrap_and_index_docs(priv.sd, encoded)
    if errors:
        # non-fatal error
        logger.error(f"could not index cache documents: {errors}")


def load_counts(ctx: context.Context, category: str, uniq: str, raw_ids: list[str]) -> dict[str, int]:
    """Load cached counts for list of ids, divided by category.

    The uniq string is added to each id during a query, to capture custom query args being used.

    Return results as dictionary of id to count.
    """
    user_security = ctx.get_user_security_unique()
    if len(raw_ids) > 10000:
        raise Exception("max counts in one call exceeded")

    ids_map = {_calc_uniq(x, uniq): x for x in raw_ids}
    # USER - read cached counts
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": category}},
                    {"terms": {"unique": list(ids_map.keys())}},
                    # same security level
                    {"term": {"user_security": user_security}},
                ],
                "should": [
                    # count was made very recently
                    {
                        "bool": {
                            "filter": [
                                {"range": {"timestamp": {"gt": "now-5m"}}},
                            ]
                        }
                    },
                    # count is over bound and was made in last day
                    {
                        "bool": {
                            "filter": [
                                {"range": {"timestamp": {"gt": "now-1d"}}},
                                {"range": {"count": {"gte": 100}}},
                            ]
                        }
                    },
                    # count is significantly over bound and was made in last week
                    {
                        "bool": {
                            "filter": [
                                {"range": {"timestamp": {"gt": "now-7d"}}},
                                {"range": {"count": {"gte": 10000}}},
                            ]
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        },
        "aggs": {
            "type_unique": {
                "terms": {"field": "type_unique", "size": 10000},
                "aggs": {"hit": {"top_hits": {"size": 1, "sort": {"docs": {"order": "desc"}}}}},
            }
        },
        "size": 0,
    }
    resp = ctx.man.cache.w.search(ctx.sd, body=body)
    counted = [x["hit"]["hits"]["hits"][0]["_source"] for x in resp["aggregations"]["type_unique"]["buckets"]]
    # get raw values and convert to original id without uniq
    results = {ids_map[row["unique"]]: row["count"] for row in counted}
    return results
