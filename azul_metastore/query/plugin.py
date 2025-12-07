"""Queries for finding plugin data."""

from __future__ import annotations

import copy
import json
import logging
import traceback
from threading import RLock
from typing import Optional

import cachetools
import pendulum
from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi

from azul_metastore.common import memcache
from azul_metastore.common.query_info import IngestError
from azul_metastore.common.utils import capture_write_stats
from azul_metastore.context import Context
from azul_metastore.encoders import plugin as plg
from azul_metastore.encoders import status as st
from azul_metastore.models import basic_events

logger = logging.getLogger(__name__)


@capture_write_stats("plugin")
def create_plugin(ctx: Context, raw_events: list[azm.PluginEvent]) -> tuple[list[IngestError], list[azm.PluginEvent]]:
    """Save list of plugin register events to opensearch."""
    results = dict()
    bad_raw_results = []
    duplicate_docs: list[azm.PluginEvent] = []
    is_has_maco = False
    # Reverse raw_results so if there are duplicate ids we get the newest event.
    for raw_event in reversed(raw_events):
        try:
            if raw_event.author.name.lower().startswith("maco") or raw_event.author.name.startswith("Maco"):
                logger.info(f"Processing event by {raw_event.author.name}")
                logger.info(raw_event.model_dump_json())
                is_has_maco = True
            # ensure event matches model
            normalised = basic_events.PluginEvent.normalise(raw_event)
            # encode with custom opensearch properties
            encoded = plg.Plugin.encode(normalised)
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
            logger.debug(
                f"There are duplicate document keys when encoding plugin events id: '{key_to_add}' for "
                + f"plugin {raw_event.author.name}-{raw_event.author.version}"
            )
            # Append the raw_event as there is a duplicate.
            duplicate_docs.append(raw_event)
            # Check if the existing data is newer than the data to be added
            # If it is keep the old data and drop the new data.
            if pendulum.parse(results[key_to_add]["timestamp"]) >= pendulum.parse(encoded["timestamp"]):
                continue
        results[key_to_add] = encoded
    # No docs to go to opensearch.
    if not results:
        if is_has_maco:
            logger.info("NO DOCS WENT TO OPENSEARCH!")
        return bad_raw_results, duplicate_docs

    if is_has_maco:
        logger.info(f"DOCS TO BE INDEXED {json.dumps(list(results.values()))}")
    doc_errors = ctx.man.plugin.w.wrap_and_index_docs(ctx.sd, results.values(), refresh=True, raise_on_errors=True)
    logger.warning("Doc bad raw results and errors:")
    logger.warning(doc_errors)
    logger.warning(json.dumps(bad_raw_results + doc_errors))
    return bad_raw_results + doc_errors, duplicate_docs


@cachetools.cached(cache=memcache.get_ttl_cache("plugins.full", ttl=60), lock=RLock(), key=lambda x: x.sd.unique())
def get_all_plugins_full(ctx: Context) -> list[models_restapi.Plugin]:
    """Find the plugin config data for all plugins."""
    resp = ctx.man.plugin.w.search(ctx.sd, body={"size": 10000})
    ret = [models_restapi.Plugin(**plg.Plugin.decode(x["_source"])) for x in resp["hits"]["hits"]]
    ret.sort(key=lambda x: (x.author.name, x.author.version))
    return ret


@cachetools.cached(cache=memcache.get_ttl_cache("plugins", ttl=60), lock=RLock(), key=lambda x: x.sd.unique())
def get_all_plugins(ctx: Context) -> list[models_restapi.LatestPluginWithVersions]:
    """Return basic information about plugins in the system."""
    body = {
        "size": 0,
        "aggs": {
            "plugin": {
                "terms": {"field": "author.name", "size": 1000},
                "aggs": {
                    "versions": {
                        "terms": {"field": "author.version", "size": 100, "order": {"newest": "desc"}},
                        "aggs": {"newest": {"max": {"field": "timestamp"}}},
                    },
                    "newest_version": {
                        "terms": {"field": "author.version", "size": 1, "order": {"newest": "desc"}},
                        "aggs": {
                            "hit": {
                                "top_hits": {
                                    "size": 1,
                                    "sort": {"timestamp": {"order": "desc"}},
                                    "_source": {"includes": ["entity"]},
                                }
                            },
                            "newest": {"max": {"field": "timestamp"}},
                        },
                    },
                },
            }
        },
    }
    resp = ctx.man.plugin.w.search(ctx.sd, body)

    result: list[models_restapi.LatestPluginWithVersions] = []
    for plugin in resp["aggregations"]["plugin"]["buckets"]:
        cur_plugin = models_restapi.LatestPluginWithVersions()
        for version in plugin["versions"]["buckets"]:
            cur_plugin.versions.append(version["key"])

        for len_one_list_of_plugin_data in plugin["newest_version"]["buckets"]:
            # Length should always be 1 or 0.
            for latest_plugin_data in len_one_list_of_plugin_data["hit"]["hits"]["hits"]:
                p_data = plg.Plugin.decode(latest_plugin_data["_source"])
                cur_plugin.newest_version = models_restapi.PluginEntity.model_validate(p_data["entity"])
                break
        result.append(cur_plugin)

    result.sort(key=lambda x: x.newest_version.name.lower())

    return result


def _plugin_stats_date_limiter() -> dict:
    """Return the date range that plugin stats should be aquired for."""
    return {"range": {"timestamp": {"gte": "now-7d/d", "lte": "now/d"}}}


@cachetools.cached(cache=memcache.get_ttl_cache("plugins.status", ttl=60), lock=RLock(), key=lambda x: x.sd.unique())
def get_all_plugin_latest_activity(ctx: Context):
    """Get a summary of the status information for plugins.

    The status summaries aren't entirely accurate due to Opensearch limitations.
    Depending which shards and nodes are queried a slightly different count can come back each time.
    """
    # Query status index for most recent completion
    recent_agg = {
        "size": 0,
        "query": {"bool": {"filter": [{"terms": {"entity.status": [x.value for x in azm.StatusEnumSuccess]}}]}},
        "aggs": {
            "plugin": {
                "terms": {"field": "encoded.author", "size": 1000},
                "aggs": {"most_recent_completion": {"max": {"field": "timestamp"}}},
            }
        },
    }
    recent_resp = ctx.man.status.w.search(ctx.sd, recent_agg)
    # Recent completion dict to map plugin name to last completed date/time
    recent = {}
    for r in recent_resp["aggregations"]["plugin"]["buckets"]:
        recent[r["key"]] = r["most_recent_completion"]["value_as_string"]

    # Get stats about statuses we care about
    success_status_types = [x.value for x in azm.StatusEnumSuccess]
    error_status_types = [x.value for x in azm.StatusEnumErrored]

    # Stats query
    stats_agg = {
        "size": 0,
        "query": {"bool": {"must": [_plugin_stats_date_limiter()]}},
        "aggs": {
            "plugin": {
                "terms": {"field": "encoded.author", "size": 500},
                # Note - doesn't filter duplicate entity values (it takes too long (minutes))
                # A duplicate is where the same binary is submitted to a plugin with a different path.
                "aggs": {"stats": {"terms": {"field": "entity.status"}}},
            }
        },
    }
    stats_resp = ctx.man.status.w.search(ctx.sd, stats_agg)
    # Construct a map of plugin_name -> stats object
    plugin_stats = {}

    ps = stats_resp["aggregations"]["plugin"]["buckets"]
    for stat in ps:
        stats = {"success": 0, "error": 0}
        d_name = stat["key"]
        # Parse success cases
        for bucket in stat["stats"]["buckets"]:
            if bucket["key"] in success_status_types:
                stats["success"] += bucket["doc_count"]
        # Parse error cases
        for bucket in stat["stats"]["buckets"]:
            if bucket["key"] in error_status_types:
                stats["error"] += bucket["doc_count"]
        # Append plugin stats
        plugin_stats[d_name] = stats

    plugin_status_data = []
    # Merge recent completion dict and plugin stats into response of first query
    for plugin in get_all_plugins(ctx):
        new_result = models_restapi.PluginStatusSummary.model_validate(plugin.model_dump())
        # Construct plugin d_name which matches the d.author field in OpenSearch
        d_name = new_result.newest_version.name + ".plugin." + new_result.newest_version.version
        # Insert last_completion information
        if d_name in recent:
            new_result.last_completion = recent[d_name]
        else:
            logger.debug(f"{d_name} not in recently completed plugins list")

        if d_name in plugin_stats:
            # Insert plugin stats information
            new_result.success_count = plugin_stats[d_name].get("success", 0)
            new_result.error_count = plugin_stats[d_name].get("error", 0)
        else:
            logger.debug(f"{d_name} not in plugin_stats list")
        # Replace with new plugin object
        plugin_status_data.append(new_result)

    return plugin_status_data


@cachetools.cached(cache=memcache.get_ttl_cache("plugins.config", ttl=60), lock=RLock(), key=lambda x: x.sd.unique())
def get_all_plugins_config(ctx: Context) -> dict[str, dict]:
    """Return the plugin config data for all plugins."""
    plugin_info_query = {
        "size": 1000,
        "_source": ["entity.name", "entity.config.*"],
        # Fetch the latest document for each plugin by timestamp
        "collapse": {"field": "entity.name"},
        "sort": [{"timestamp": {"order": "desc"}}, {"entity.name": {"order": "asc"}}],
        "from": 0,
    }
    plugin_res = ctx.man.plugin.w.search(ctx.sd, body=plugin_info_query)
    # load and decode json config strings into raw values
    plugin_name_config: dict[str, dict] = {
        doc["_source"]["entity"]["name"]: doc["_source"]["entity"]["config"]
        for doc in plugin_res["hits"]["hits"]
        if "config" in doc["_source"]["entity"]
    }
    for cfg in plugin_name_config.values():
        for k in cfg:
            cfg[k] = json.loads(cfg[k])

    return plugin_name_config


def get_author_stats(ctx: Context, name: str, version: str) -> list[models_restapi.StatusGroup]:
    """Return statistics for author (Uses cardinality to remove duplicate values).

    This function produces more accurate results than the plugin summary information because it filters out
    duplicate submissions of the same entity to a plugin.
    A duplicate is where the same binary is submitted to a plugin with a different path.
    """
    body = {
        "size": 0,
        "query": {
            "bool": {
                "must": [_plugin_stats_date_limiter()],
                "filter": [
                    {"term": {"author.name": name}},
                    {"term": {"author.version": version}},
                ],
            }
        },
        "aggs": {
            "statuses": {
                "terms": {"field": "entity.status"},
                "aggs": {
                    "count": {
                        "cardinality": {"field": "entity.input.entity.sha256", "precision_threshold": 100},
                    },
                    "entities": {
                        "terms": {"field": "entity.input.entity.sha256", "size": 100, "order": {"timestamp": "desc"}},
                        "aggs": {
                            "timestamp": {"max": {"field": "timestamp"}},
                            "hit": {
                                "top_hits": {
                                    "size": 1,
                                    "sort": {"timestamp": {"order": "desc"}},
                                }
                            },
                        },
                    },
                },
            }
        },
    }
    resp = ctx.man.status.w.search(ctx.sd, body=body)
    aggs = resp["aggregations"]
    data = {}
    for row in aggs["statuses"]["buckets"]:
        status = row["key"]
        data[status] = models_restapi.StatusGroup(status=status, num_items=row["count"]["value"], items=[])
        for row2 in row["entities"]["buckets"]:
            data[status].items.append(
                models_restapi.StatusEvent(**st.Status.decode(row2["hit"]["hits"]["hits"][0]["_source"]))
            )
    ret = sorted(data.values(), key=lambda x: x.status)
    return ret


def get_plugin(
    ctx: Context,
    name: str,
    version: str,
) -> Optional[dict]:
    """Return doc for plugin."""
    body = {
        "query": {"bool": {"filter": [{"term": {"author.name": name}}, {"term": {"author.version": version}}]}},
        "size": 1,
    }
    resp = ctx.man.plugin.w.search(ctx.sd, body=body)
    hits = resp["hits"]["hits"]
    if not hits:
        return None
    return plg.Plugin.decode(hits[0]["_source"]["entity"])


def get_raw_feature_names(ctx: Context) -> list[str]:
    """Return features that plugins are able to write to the system."""
    basic = ["magic", "mime", "filename", "file_format_legacy"]
    body = {
        "size": 0,
        "aggs": {
            "author": {
                "terms": {"field": "author.name", "size": 10000},
                "aggs": {
                    "hit": {
                        "top_hits": {
                            "size": 1,
                            "_source": {"includes": ["encoded.features.name"]},
                            "sort": {"timestamp": {"order": "desc"}},
                        }
                    }
                },
            }
        },
    }
    resp = ctx.man.plugin.w.search(ctx.sd, body=body)
    loaded = [
        y.get("name")
        for x in resp["aggregations"]["author"]["buckets"]
        for y in x["hit"]["hits"]["hits"][0]["_source"].get("entity", {}).get("features", [])
    ]
    loaded = [x for x in loaded if x]
    return sorted(set(basic + loaded))


def find_features(ctx: Context, *, filters: list[dict] = None) -> list[models_restapi.Feature]:
    """Find all features in metastore, and returns approximate counts."""
    if not filters:
        filters = []
    ret = {}
    # read descriptions
    # Manual castings should be ok as there are not that many plugins

    body = {
        "query": {"bool": {"filter": filters}},
        "size": 0,
        "aggs": {
            "name": {
                "terms": {"field": "author.name", "size": 10000},
                "aggs": {
                    "version": {
                        "terms": {"field": "author.version", "size": 20},
                        "aggs": {
                            "hit": {
                                "top_hits": {
                                    "size": 1,
                                    "sort": {"timestamp": "desc"},
                                }
                            }
                        },
                    }
                },
            }
        },
    }
    resp = ctx.man.plugin.w.search(ctx.sd, body=body)
    docs = []
    for row in resp["aggregations"]["name"]["buckets"]:
        for row2 in row["version"]["buckets"]:
            src = row2["hit"]["hits"]["hits"][0]["_source"]
            docs.append(plg.Plugin.decode(src))
    feats = []
    for doc in docs:
        _base = dict(
            author_type=doc["author"]["category"],
            author_name=doc["author"]["name"],
            author_version=doc["author"]["version"],
            security=doc["security"],
        )
        for feat in doc["entity"].get("features", []):
            feat.update(copy.deepcopy(_base))
            feats.append(feat)

    feats.sort(key=lambda x: (x["name"], x["author_type"], x["author_name"], x["author_version"]))

    for feat in feats:
        f = feat["name"]
        ret.setdefault(f, {"name": f}).setdefault("descriptions", []).append(feat)

    for res in ret.values():
        if "descriptions" not in res:
            continue

        tags = []
        secs = []
        for desc in res["descriptions"]:
            tags += desc.pop("tags", [])
            secs.append(desc.pop("security"))
            desc.pop("name")

        res["tags"] = sorted(set(tags))
        res["security"] = sorted(set(secs))

    ret = [models_restapi.Feature(**x) for x in ret.values()]

    return sorted(ret, key=lambda x: x.name)
