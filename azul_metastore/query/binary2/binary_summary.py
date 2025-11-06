"""Queries for reading a summary of metadata for a specific binary."""

import copy
import logging

from azul_bedrock import models_restapi
from azul_bedrock.models_restapi.binaries import BinaryMetadataDetail as Detail

from azul_metastore import context, settings
from azul_metastore.common import utils
from azul_metastore.encoders import binary2 as rc
from azul_metastore.query import annotation, plugin

from . import binary_related

logger = logging.getLogger(__name__)


def read(
    ctx: context.Context,
    sha256: str,
    details: list[Detail] = None,
    author: str | None = None,
    bucket_size=100,
) -> models_restapi.BinaryMetadata:
    """Read all critical metadata for a given binary."""
    if not details:
        # default to returning all detail information
        details = [x.value for x in Detail]

    sha256 = sha256.lower()
    ret = _read_within(ctx, sha256, details=details, bucket_size=bucket_size, author=author)

    if Detail.children in details:
        # get children
        ret.children = binary_related.read_children(ctx, sha256)
        if len(ret.children) >= bucket_size:
            ret.diagnostics.append(_generate_diagnostic("children", bucket_size))

    if Detail.feature_tags in details:
        annotation.add_feature_value_tags(ctx, ret.features)

    if Detail.tags in details:
        # read any tags present for the binary
        ret.tags = annotation.read_binary_tags(ctx, sha256)

    return ret


def _read_within(
    ctx: context.Context,
    sha256: str,
    details: list[models_restapi.BinaryMetadataDetail] = None,
    author: str | None = None,
    bucket_size=100,
) -> models_restapi.BinaryMetadata:
    """Return plugin information about a specific binary."""
    if not details:
        # default to returning all detail information
        details = [x.value for x in Detail]

    if Detail.feature_tags in details:
        # requires features to enrich tags on
        details.append(Detail.features)

    body = {
        # usually caps at 10k but we want the full count when detail is requested
        "track_total_hits": True if Detail.total_hits in details else 10000,
        # don't want individual docs
        "size": 0,
        # only aggregate on docs for current entity
        "query": {"bool": {"filter": [{"term": {"sha256": sha256}}]}},
        "aggs": {
            # get newest timestamp across all docs
            "NEWEST": {"max": {"field": "timestamp"}},
            # get unique security strings across all events seen for the binary
            "SECURITY": {
                "terms": {"field": "security", "size": bucket_size},
            },
            # get a few variants for every source
            "SOURCES": {
                "terms": {"field": "source.name", "size": bucket_size},
                "aggs": {
                    "VARIANT": {
                        # sort by depth to prefer directly sourced over indirectly sourced
                        "terms": {
                            "field": "track_source_references",
                            "size": bucket_size,
                            "order": {"DEPTH": "asc"},
                        },
                        "aggs": {
                            "HITS": {
                                "top_hits": {
                                    "size": 1,
                                    "_source": {"includes": rc.fields_submission},
                                }
                            },
                            "DEPTH": {"min": {"field": "depth"}},
                        },
                    }
                },
            },
            # get docs with unique sets of feature values
            "FEATURES": {
                "terms": {"field": "uniq_features", "size": bucket_size},
                "aggs": {
                    "HITS": {
                        "top_hits": {
                            "size": 1,
                            "_source": {"includes": ["features", "sha256_author_action"]},
                        }
                    }
                },
            },
            # get docs with unique info blocks
            "INFO": {
                "terms": {"field": "uniq_info", "size": bucket_size},
                "aggs": {
                    "HITS": {
                        "top_hits": {
                            "size": 1,
                            "_source": {"includes": ["info", "sha256_author_action"]},
                        }
                    }
                },
            },
            # get docs with unique data blocks
            "DATASTREAMS": {
                "terms": {"field": "uniq_data", "size": bucket_size},
                "aggs": {
                    "HITS": {
                        "top_hits": {
                            "size": 1,
                            "_source": {"includes": ["datastreams", "sha256_author_action"]},
                        }
                    }
                },
            },
            # get docs with unique author+event+stream
            "INSTANCES": {
                "terms": {"field": "sha256_author_action", "size": bucket_size},
                "aggs": {
                    "HITS": {
                        "top_hits": {
                            "size": 1,
                            "_source": {
                                "includes": [
                                    "author",
                                    "action",
                                    "sha256_author_action",
                                    "num_feature_values",
                                ]
                            },
                        }
                    }
                },
            },
            "PARENTS": {
                "terms": {
                    "field": "parent.sha256",
                    "size": bucket_size,
                    "order": {"NEWEST": "desc"},
                },
                "aggs": {
                    "HITS": {
                        "top_hits": {
                            "size": 1,
                            "_source": {"includes": rc.fields_link + rc.fields_recover_source_binary_node},
                        }
                    },
                    "NEWEST": {"max": {"field": "timestamp"}},
                },
            },
        },
    }
    if author:
        body["query"]["bool"]["filter"].append({"term": {"author.name": author}})

    # remove aggregations that are not needed
    if Detail.documents not in details:
        body["aggs"].pop("NEWEST")
    if Detail.security not in details:
        body["aggs"].pop("SECURITY")
    if Detail.sources not in details:
        body["aggs"].pop("SOURCES")
    if Detail.features not in details:
        body["aggs"].pop("FEATURES")
    if Detail.info not in details:
        body["aggs"].pop("INFO")
    if Detail.datastreams not in details:
        body["aggs"].pop("DATASTREAMS")
    if Detail.instances not in details:
        body["aggs"].pop("INSTANCES")
    if Detail.parents not in details:
        body["aggs"].pop("PARENTS")

    resp = ctx.man.binary2.w.search(ctx.sd, body=body, routing=sha256)

    ret = models_restapi.BinaryMetadata(documents=models_restapi.BinaryDocuments(count=resp["hits"]["total"]["value"]))
    if ret.documents.count > 0 and Detail.documents in details:
        ret.documents.newest = resp["aggregations"]["NEWEST"]["value_as_string"]
    if Detail.security in details:
        ret.security = _parse_security(resp)
    if Detail.sources in details:
        ret.sources = _parse_sources(resp)
    if Detail.features in details:
        ret.features = _parse_features(resp)
    if Detail.info in details:
        ret.info = _parse_info(resp)
    if Detail.datastreams in details:
        ret.streams = _parse_streams(resp)
    if Detail.instances in details:
        ret.instances = _parse_instances(resp)
    if Detail.parents in details:
        ret.parents = _parse_parents(resp)

    # Enrich entities with detailed info
    event_count = resp["hits"]["total"]["value"]
    diagnostics = _parse_diagnostics(
        ctx, resp=resp, meta=ret, event_count=event_count, bucket_size=bucket_size, details=details
    )
    if diagnostics:
        ret.diagnostics = diagnostics

    return ret


def _generate_diagnostic(title: str, bucket_size: int) -> models_restapi.BinaryDiagnostic:
    """Generates a diagnostic for bucket size exceeded."""
    return models_restapi.BinaryDiagnostic(
        severity="warning",
        id=f"many_buckets_{title}",
        title=f"Results may be missing for {title}",
        body=f"There were more fields identified by the database (over {bucket_size}). "
        "Consider increasing query size in user settings.",
    )


def _parse_diagnostics(
    ctx: context.Context,
    resp: dict,
    meta: models_restapi.BinaryMetadata,
    event_count: int,
    bucket_size: int,
    details: list[models_restapi.BinaryMetadataDetail],
) -> list[models_restapi.BinaryDiagnostic]:
    s = settings.get()
    diagnostics: list[models_restapi.BinaryDiagnostic] = []

    def _bucket_size_check(title: str, buckets: list):
        if len(buckets) >= bucket_size:
            diagnostics.append(_generate_diagnostic(title, bucket_size))

    if Detail.security in details:
        _bucket_size_check("security", resp["aggregations"]["SECURITY"]["buckets"])
    if Detail.sources in details:
        _bucket_size_check("sources", resp["aggregations"]["SOURCES"]["buckets"])
    if Detail.features in details:
        _bucket_size_check("features", resp["aggregations"]["FEATURES"]["buckets"])
    if Detail.info in details:
        _bucket_size_check("info", resp["aggregations"]["INFO"]["buckets"])
    if Detail.datastreams in details:
        _bucket_size_check("datastreams", resp["aggregations"]["DATASTREAMS"]["buckets"])
    if Detail.instances in details:
        _bucket_size_check("instances", resp["aggregations"]["INSTANCES"]["buckets"])
    if Detail.parents in details:
        _bucket_size_check("parents", resp["aggregations"]["PARENTS"]["buckets"])

    # we need to check for each source
    if Detail.sources in details:
        aggs = resp["aggregations"]["SOURCES"]["buckets"]
        for bucket in aggs:
            _bucket_size_check(f"sources_{bucket['key']}_references", bucket["VARIANT"]["buckets"])

    if event_count >= s.warn_on_event_count:
        diagnostics.append(
            models_restapi.BinaryDiagnostic(
                severity="warning",
                id="many_events",
                title="Many events",
                body=f"This binary has many events in the system ({event_count}) and accurate"
                " results can't be guaranteed.",
            )
        )

    if Detail.datastreams in details:
        # Some checks can only be performed on files that have data
        file_size: int | None = None
        for data_document in meta.streams:
            # Validate that there is a 'content' label for this stream, indicating that
            # this is actually the content of the current file and not an altstream
            if "content" in data_document.label:
                file_size = data_document.size
                break

        if file_size is not None:
            plugin_name_config = plugin.get_all_plugins_config(ctx)
            oversize_file_plugins = []
            for name, config in plugin_name_config.items():
                if config.get("filter_max_content_size", 0) != 0 and config["filter_max_content_size"] < file_size:
                    oversize_file_plugins.append(name)
                elif config.get("filter_max_content_size") is None:
                    # The filter_max_content_size should be present on all plugins if it
                    # isn't it means the plugin is misconfigured.
                    logger.warning("filter_max_content_size was missing on plugin config for the plugin %s", name)

            if len(oversize_file_plugins) > 0:
                # FUTURE this should be done with a i18n library, ideally on the webui/azul client side
                #        (though this should be done across the board, not just for these alerts)
                plugins_plural = "plugins" if len(oversize_file_plugins) > 1 else "plugin"
                title = f"Content too large for {len(oversize_file_plugins)} {plugins_plural}"
                body = f"The following {plugins_plural} will not process the binary: "
                if len(oversize_file_plugins) > 5:
                    body += ", ".join(oversize_file_plugins[0:5])
                    body += f" and {len(oversize_file_plugins) - 5} more"
                else:
                    body += ", ".join(oversize_file_plugins)
                diagnostics.append(
                    models_restapi.BinaryDiagnostic(severity="warning", id="large", title=title, body=body)
                )
        else:
            diagnostics.append(
                models_restapi.BinaryDiagnostic(
                    severity="info",
                    id="no_content",
                    title="Content not found",
                    body="The content for this binary is not in Azul and as such "
                    "this file is unlikely to be processed by most plugins.",
                )
            )

    diagnostics.sort(key=lambda x: x.id)
    return diagnostics


def _parse_security(resp: dict) -> list[str]:
    buckets = resp["aggregations"]["SECURITY"]["buckets"]
    return utils.azsec().string_rank(x["key"] for x in buckets)


def _parse_sources(resp: dict) -> list[models_restapi.BinarySource]:
    buckets = resp["aggregations"]["SOURCES"]["buckets"]
    sources = []
    for bucket1 in buckets:
        direct = []
        indirect = []
        for bucket2 in bucket1["VARIANT"]["buckets"]:
            row = bucket2["HITS"]["hits"]["hits"][0]["_source"]
            depth = int(bucket2["DEPTH"]["value"])
            rc.Binary2.decode(row)
            source = row["source"]
            source.pop("path", None)  # decoding results in a path regeneration
            source["track_source_references"] = row["track_source_references"]
            if depth > 0:
                indirect.append(source)
            else:
                direct.append(source)
        sources.append(
            models_restapi.BinarySource(
                source=bucket1["key"],
                direct=sorted(direct, key=lambda x: x["timestamp"], reverse=True),
                indirect=sorted(indirect, key=lambda x: x["timestamp"], reverse=True),
            )
        )

    sources.sort(key=lambda x: x.source)
    return sources


def _parse_features(resp: dict) -> list[models_restapi.BinaryFeatureValue]:
    buckets = resp["aggregations"]["FEATURES"]["buckets"]
    raw_features = []
    for bucket in buckets:
        bin = bucket["HITS"]["hits"]["hits"][0]["_source"]
        if not bin.get("features"):
            continue
        # must set instance keys so we can trace which plugin a feature/stream/etc came from
        for x in bin["features"]:
            # offset and size are handled within the 'location' part
            # and it doesn't make sense to return a list of offsets or sizes as they depend on each other
            x.pop("offset", None)
            x.pop("size", None)
            # rename 'extra' key
            x["enriched"] = enriched = x.pop("enriched", {})
            x["encoded"] = x.pop("encoded", {})
            x["instances"] = bin["sha256_author_action"]
            filepath = enriched.get("filepath")
            if filepath:
                # tree'ize filepaths
                sunix = filepath.split("/")
                for i in range(min(len(sunix) - 1, 5)):
                    enriched.setdefault("filepath_unix", []).append("/".join(sunix[: i + 1]))
                    enriched.setdefault("filepath_unixr", []).append("/".join(sunix[-(i + 1) :]))

        raw_features += bin["features"]

    # merge features with same basic info (feature, value, type, label)
    features = []
    feature_sets = {}
    # assemble list of occurrences of same feature values
    for f in raw_features:
        uniq = f["name"] + f["type"] + str(f["value"])
        feature_sets.setdefault(uniq, []).append(f)

    # for each list of similar feature values, merge authors and such together
    for fs in feature_sets.values():
        # copy basic feature value information
        feature = copy.deepcopy(fs[0])
        feature.pop("encoded")
        feature["parts"] = feature.pop("enriched")
        # must make sure to overwrite existing properties with aggregate lists (label, location, etc)
        # all authors & actions
        feature["instances"] = sorted(set(f["instances"] for f in fs))
        # all locations
        feature["parts"]["location"] = sorted(
            {
                (f["encoded"]["location"]["gte"], f["encoded"]["location"]["lte"])
                for f in fs
                if f["encoded"].get("location")
            }
        )
        if not feature["parts"]["location"]:
            feature["parts"].pop("location")
        # all labels
        feature["label"] = sorted({f["label"] for f in fs if f.get("label")})
        # add the merged feature to output list
        features.append(models_restapi.BinaryFeatureValue(**feature))

    features.sort(key=lambda x: str(x.value))
    features.sort(key=lambda x: x.name)
    return features


def _parse_info(resp: dict) -> list[models_restapi.BinaryInfo]:
    buckets = resp["aggregations"]["INFO"]["buckets"]
    infos = []
    for bucket in buckets:
        bin = bucket["HITS"]["hits"]["hits"][0]["_source"]
        raw_info = bin.get("info")
        if not raw_info:
            continue
        # gotta wrap it up since we don't know the structure (can't merge dupes either)
        infos.append(models_restapi.BinaryInfo(info=raw_info, instance=bin["sha256_author_action"]))
    return infos


def _parse_streams(resp: dict) -> list[models_restapi.DatastreamInstances]:
    buckets = resp["aggregations"]["DATASTREAMS"]["buckets"]
    streams = {}
    for bucket in buckets:
        bin = bucket["HITS"]["hits"]["hits"][0]["_source"]
        raw_streams = bin.get("datastreams", [])
        for basestream in raw_streams:
            label = basestream["label"]
            basestream["label"] = set()
            basestream["instances"] = []
            stream = streams.setdefault(basestream["sha256"], basestream)
            stream["label"].add(label)
            stream["instances"].append(bin["sha256_author_action"])
    for stream in streams.values():
        stream["label"] = sorted(stream["label"])
    return [models_restapi.DatastreamInstances(**x) for x in sorted(streams.values(), key=lambda x: x["label"][0])]


def _parse_instances(resp: dict) -> list[models_restapi.EntityInstance]:
    buckets = resp["aggregations"]["INSTANCES"]["buckets"]
    instances = []
    for bucket in buckets:
        row = bucket["HITS"]["hits"]["hits"][0]["_source"]
        instance = {
            "key": row["sha256_author_action"],
            "author": row["author"],
            "action": row["action"],
            "num_feature_values": row.get("num_feature_values", 0),
        }
        instances.append(instance)
    return [models_restapi.EntityInstance(**x) for x in sorted(instances, key=lambda x: x["key"])]


def _parse_parents(resp: dict) -> list[models_restapi.PathNode]:
    parents = []
    for bucket in resp["aggregations"]["PARENTS"]["buckets"]:
        # Return the child node info with the parent sha256.
        # This is because we care about how the child was produce from the parent,
        # not how the parent was produced.
        row = bucket["HITS"]["hits"]["hits"][0]["_source"]
        nc = rc.Binary2.recover_source_binary_node(row)
        node = row["parent"]
        node.pop("encoded", None)
        node["track_link"] = row["track_link"]
        # parent has info to its parent, which must be replaced
        node["author"] = nc["author"]
        node["relationship"] = nc["relationship"]
        node["action"] = nc["action"]
        node["timestamp"] = nc["timestamp"]
        parents.append(models_restapi.PathNode(**node))
    return parents
