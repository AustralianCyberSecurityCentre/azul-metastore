"""Controls converting data from opensearch-land to azul-land and vice versa.

This involves converting structures to opensearch compatible dictionaries.
"""

from __future__ import annotations

import copy
import json
import logging
import re
from collections import defaultdict

import cachetools
import xxhash
from azul_bedrock import models_network as azm

from azul_metastore import settings
from azul_metastore.common import feature, memcache
from azul_metastore.common.tlsh import encode_tlsh_into_vector
from azul_metastore.common.utils import BadSourceException, azsec, md5, to_utc
from azul_metastore.encoders import base_encoder, template_feature, template_node

from .base_encoder import uid

logger = logging.getLogger(__name__)

cache_ids: cachetools.TTLCache = None


def reset_doc_id_cache():
    """Returns a new doc id cache."""
    global cache_ids
    s = settings.get()

    # xxhash 128 is 16 bytes, collisions should be very rare
    # timestamp 'should' be a 64 bit number, and we add another 64 bits for the ttl timestamp)
    # not sure how much extra ram used to manage the data structure - linked list?
    # we are not using xxhash for security so manufactured collisions are not a concern
    # assume each entry takes 64 bytes to be safe (probably lower)
    cache_ids = memcache.get_lru_cache("doc_id", maxsize=s.binary2_cache_count)


reset_doc_id_cache()

map_common = {
    "timestamp": {"type": "date"},
    "action": {"type": "keyword"},
    "track_author": {"type": "keyword"},
    "author": {
        "properties": {
            "security": {"type": "keyword"},
            "category": {"type": "keyword"},
            "name": {"type": "keyword"},
            "version": {"type": "keyword"},
        },
        "type": "object",
    },
    # file hard hashes
    "sha256": {"eager_global_ordinals": True, "type": "keyword"},
    "sha512": {"type": "keyword"},
    "sha1": {"type": "keyword"},
    "md5": {"type": "keyword"},
    # file fuzzy hashes
    "ssdeep": {"type": "keyword"},
    "encoded_ssdeep": {
        "properties": {
            "blocksize": {"type": "integer"},
            "chunk": {"type": "text", "analyzer": "ssdeep_ngram_analyzer"},
            "dchunk": {"type": "text", "analyzer": "ssdeep_ngram_analyzer"},
        },
        "type": "object",
    },
    "tlsh": {"type": "keyword"},
    "tlsh_vector": {
        "type": "knn_vector",
        "dimension": 36,
        "data_type": "byte",
        # https://opensearch.org/docs/latest/field-types/supported-field-types/knn-spaces/
        "space_type": "cosinesimil",
        "method": {"name": "hnsw", "engine": "lucene"},
    },
    # file identification
    "mime": {"type": "keyword"},
    "magic": {"type": "keyword"},
    "file_format": {"type": "keyword"},
    "file_format_legacy": {"type": "keyword"},  # deprecated type identification
    "file_extension": {"type": "keyword"},  # expected file type extension
    "size": {"type": "unsigned_long"},  # support large files via unsigned_long
    # information about all datastreams relevant to current event
    "datastreams": {
        "properties": {
            "identify_version": {"type": "integer"},
            # content vs pcap vs anything
            "label": {"type": "keyword"},
            # file hard hashes
            "sha256": {"type": "keyword"},  # should be unique in result
            "sha512": {"type": "keyword"},
            "sha1": {"type": "keyword"},
            "md5": {"type": "keyword"},
            # file fuzzy hashes
            "ssdeep": {"type": "keyword"},
            "tlsh": {"type": "keyword"},
            # file identification
            "mime": {"type": "keyword"},
            "magic": {"type": "keyword"},
            "file_format": {"type": "keyword"},
            "file_format_legacy": {"type": "keyword"},  # deprecated type identification
            "file_extension": {"type": "keyword"},  # expected file type extension
            "size": {"type": "unsigned_long"},  # support large files via unsigned_long
            # if text, the programming language
            "language": {"type": "keyword"},
        },
        "type": "object",
    },
    "features": {"properties": template_feature.map_feature, "type": "object"},
    # dictionary with non specific key definitions
    "info": {"enabled": False, "type": "object"},
    #
    # Derived info, not part of original events
    #
    # FUTURE we index features twice which effectively doubles disk usage and ingest time
    # mapped dynamically
    "features_map": {"type": "object", "properties": {}},
    "filename": {"type": "keyword"},
    # number of unique feature names
    "num_feature_names": {"type": "integer"},
    # number of unique feature values
    "num_feature_values": {"type": "integer"},
    # misc combinations to assist aggregation
    "sha256_author_action": {"type": "keyword"},
    "uniq_features": {"eager_global_ordinals": True, "type": "keyword"},
    "uniq_info": {"eager_global_ordinals": True, "type": "keyword"},
    "uniq_data": {"eager_global_ordinals": True, "type": "keyword"},
}

map_link = {
    "track_link": {"type": "keyword"},
    "parent_track_author": {"type": "keyword"},
    "parent_relationship": template_node.map_node["relationship"],
    "parent": {"properties": template_node.map_node, "type": "object"},
}
fields_link = [x for x in map_link.keys()]

map_submission = {
    "depth": {"type": "integer"},
    "track_source_references": {"type": "keyword"},
    "source": {
        "properties": {
            "security": {"type": "keyword"},
            "name": {"type": "keyword"},
            "timestamp": {"type": "date"},
            "settings": {"enabled": False, "type": "object"},
            "encoded_settings": {
                "properties": {
                    "key": {"type": "keyword"},
                    "value": {"type": "keyword"},
                },
                "type": "object",
            },
            "references": {"enabled": False, "type": "object"},
            "encoded_references": {
                "properties": {
                    "key": {"type": "keyword"},
                    "value": {"type": "keyword"},
                    "key_value": {"type": "keyword"},
                },
                "type": "object",
            },
        },
        "type": "object",
    },
}
fields_submission = [x for x in map_submission.keys()]

fields_recover_source_binary_node = [
    "author",
    "action",
    "timestamp",
    "sha256",
    "file_format_legacy",
    "file_format",
    "size",
    "parent_relationship",
]

# one mapping template is used by parents and all children
map_binary = {
    "_routing": {"required": True},
    "dynamic": "strict",
    "properties": {
        # mapping from parent binary to child info types
        # all 'metadata' to allow a single parent-child query across all subdocs
        "binary_info": {"type": "join", "relations": {"binary": ["metadata"]}},
        "security": {"type": "keyword"},
        "encoded_security": base_encoder.get_security_mapping(),
        # below fields are for 'metadata' only as 'binary' parent is a hollow doc
        **map_common,  # always present
        **map_submission,  # not present for enriched / augmented
        **map_link,  # not present for enriched / augmented / sourced / mapped
    },
}

_valid_pattern = re.compile("[^0-9a-zA-Z.]+")


def _remove_invalid_chars(invalid: str) -> str:
    """Remove any non alphanumeric and non full stop characters from a string and return the clean string."""
    return _valid_pattern.sub("", invalid)


class Binary2(base_encoder.BaseIndexEncoder):
    """Converter for a result."""

    docname = "binary2"
    index_settings = {
        "number_of_shards": 3,
        "number_of_replicas": 2,
        "refresh_interval": "30s",
        "analysis": {
            "analyzer": {
                "path": {"tokenizer": "hierarchy"},
                "path_reversed": {"tokenizer": "hierarchy_reversed"},
                "pathw": {"tokenizer": "hierarchyw"},
                "pathw_reversed": {"tokenizer": "hierarchyw_reversed"},
                "alphanumeric": {"tokenizer": "alphanumeric"},
                "ssdeep_ngram_analyzer": {"tokenizer": "ssdeep_tokenizer"},
            },
            "tokenizer": {
                "hierarchy": {"type": "path_hierarchy", "delimiter": "/"},
                "hierarchy_reversed": {"type": "path_hierarchy", "delimiter": "/", "reverse": "true"},
                "hierarchyw": {"type": "path_hierarchy", "delimiter": "\\"},
                "hierarchyw_reversed": {"type": "path_hierarchy", "delimiter": "\\", "reverse": "true"},
                "alphanumeric": {"type": "char_group", "tokenize_on_chars": ["whitespace", "punctuation", "symbol"]},
                "ssdeep_tokenizer": {
                    "type": "ngram",
                    "min_gram": 7,
                    "max_gram": 7,
                    "token_chars": ["letter", "digit", "punctuation"],
                },
            },
        },
        "index": {
            "mapping": {"total_fields": {"limit": 20000}},
            "knn": True,
            "knn.derived_source": {"enabled": False},
        },
    }
    mapping = map_binary

    @classmethod
    def encode_feature(cls, feat: dict) -> dict:
        """Encode feature with extra attributes for opensearch."""
        feat["encoded"] = {}
        # offset in file
        if "offset" in feat or "size" in feat:
            gte = feat.get("offset", 0)
            lte = gte + feat.get("size", 0)
            feat["encoded"]["location"] = {"gte": gte, "lte": lte}
        return feat

    @classmethod
    def _encode_submission(cls, event: dict, unique_submission: str) -> dict:
        """Submission tracks source information for the binary."""
        depth = len(event["source"]["path"]) - 1
        # Augmented events should have the same depth as the sourced event as they aren't additional depth.
        # They are more similar to enrichment events but source tracking is needed for deletion.
        if event["action"] == azm.BinaryAction.Augmented:
            depth -= 1
            # This shouldn't normally be used as augmented events should always have a source path of 2 of more.
            # Because there should be a sourced event + the plugin that produced the augmented event.
            if depth < 0:
                depth = 0
        ret = {
            "_id": unique_submission,
            "depth": depth,
            "source": copy.deepcopy(event["source"]),
            "track_source_references": event["track_source_references"],
        }
        ret["source"].pop("path")
        return ret

    @classmethod
    def _encode_link(cls, event: dict, unique_link: str) -> dict:
        """Link tracks a relationship between two binaries."""
        ret = {
            # distinct if entity differs in parent/child, or diff author/event for child
            "_id": unique_link,
            "track_link": event["track_links"][-1],
            "parent": copy.deepcopy(event["source"]["path"][-2]),
            "parent_track_author": event["track_authors"][-2],
            # childs relationship to parent
            "parent_relationship": copy.deepcopy(event["source"]["path"][-1].get("relationship", {})),
        }

        return ret

    @classmethod
    def encode(cls, event: dict) -> dict:
        """Encode to opensearch layer format.

        Does not perform normalisation, that should occur as part of the models/basic_events.py.
        """
        # ensure we can purge the document
        # track links is empty if this is a top level doc, so not checked here
        mandatory = ["track_source_references", "track_authors"]
        for item in mandatory:
            if item not in event or not event[item]:
                raise Exception(f"event is missing tracking information '{item}': {event}")

        # Copy whole event to avoid issue when event is modified or nested components are used in the encoded output.
        event = copy.deepcopy(event)
        # embed entity at root level
        encoded_event = event["entity"]

        # Get necessary keys to ensure if they are missing they fail early
        event_source = event["source"]
        event_author = event["author"]
        event_source_path = event_source["path"]

        # security of a result document is the combination of source, author and link security
        event["security"] = azsec().string_combine(
            [
                event_source["security"],
                event_author["security"],
                *(x["author"]["security"] for x in event_source_path),
            ]
        )
        cls._encode_security(event)

        # create data structures for d data
        for node in event_source_path:
            node["encoded"] = {}

        # ensure valid source
        source_id = event_source["name"]
        if not settings.check_source_exists(source_id):
            raise BadSourceException(f"Source does not exist: {source_id}")

        for k, v in event_source.get("settings", {}).items():
            event_source.setdefault("encoded_settings", []).append({"key": k, "value": v})
        event_source.get("encoded_settings", []).sort(key=lambda x: x["key"])

        # source data has to be converted to known mapping
        for k, v in event_source.get("references", {}).items():
            event_source.setdefault("encoded_references", []).append({"key": k, "value": v, "key_value": f"{k}.{v}"})
        event_source.get("encoded_references", []).sort(key=lambda x: x["key_value"])

        # timestamp may not be in utc format
        # FUTURE may not be necessary
        event_source["timestamp"] = to_utc(event_source["timestamp"])
        for node in event_source_path:
            node["timestamp"] = to_utc(node["timestamp"])

        # sort streams by sha256 (necessary for consistency and uniq_data calculation)
        if "datastreams" in encoded_event:
            encoded_event["datastreams"] = sorted(encoded_event["datastreams"], key=lambda x: x["sha256"])

        # write ssdeep hash based on ssdeep in info
        ssdeep = encoded_event.get("ssdeep") or encoded_event.get("info", {}).get("ssdeep")
        if ssdeep:
            try:
                blockSizeStr, chunk, doubleChunk = ssdeep.split(":")
                blockSize = int(blockSizeStr)
            except ValueError:
                raise Exception(f"ssdeep could not be parsed {ssdeep}")
            encoded_event["ssdeep"] = ssdeep
            encoded_event["encoded_ssdeep"] = {
                "blocksize": blockSize,
                "chunk": chunk,
                "dchunk": doubleChunk,
            }

        # index tlsh into a searchable vector
        tlsh = encoded_event.get("tlsh") or encoded_event.get("info", {}).get("tlsh")
        if tlsh:
            result = encode_tlsh_into_vector(tlsh)
            if result:
                encoded_event["tlsh_vector"] = result

        for node in event_source_path:
            laggs = node["encoded"]
            node_author = uid(node["author"]["category"], node["author"]["name"])
            laggs["sha256_author_action"] = uid(node["sha256"], node_author, node["action"])

        # compute combinations of fields that might be useful for aggregation
        base_author = uid(event_author["category"], event_author["name"])
        sha256_author_action_uid = uid(encoded_event["sha256"], base_author, event["action"])
        encoded_event["sha256_author_action"] = sha256_author_action_uid

        if encoded_event.get("info"):
            encoded_event["uniq_info"] = uid(sha256_author_action_uid, md5(json.dumps(encoded_event["info"])))

        if "features" in encoded_event and len(encoded_event["features"]):
            for feat in encoded_event.get("features", []):
                # parse feature values into multiple fields
                try:
                    feature.enrich_feature(feat)
                except feature.FeatureEncodeException as e:
                    __loggable_author = (
                        f"{event.get('author', {}).get('name')}-{event.get('author', {}).get('version')}"
                    )
                    logger.error(
                        f"enriching feature failed (feature will still be encoded without enrichment) with error '{e}'"
                        + f" for entity with id '{event.get('entity', {}).get('sha256')}' originating from plugin "
                        + __loggable_author,
                        extra={"error_type": "feature_encoding", "author": __loggable_author, "error": str(e)},
                    )

                # encode feature for searching
                cls.encode_feature(feat)
                # enrich quick read properties
                if feat["name"] in ["filename"]:
                    encoded_event.setdefault(feat["name"], []).append(feat["value"])

            raw_features = encoded_event["features"]
            features = {}
            feature_values = []
            for row in raw_features:
                features.setdefault(row["name"], []).append(row["value"])
                feature_values.append(f"{row['name']}.{row['value']}")
            for v in features.values():
                v.sort()
            feature_values.sort()
            feature_names = sorted({str(x["name"]) for x in raw_features})
            # store data
            features_map = defaultdict(list)
            for feat in raw_features:
                features_map[feat.get("name")].append(feat.get("value"))

            encoded_event.update(
                {
                    "features_map": features_map,
                    "num_feature_names": len(feature_names),
                    "num_feature_values": len(raw_features),
                    "uniq_features": uid(sha256_author_action_uid, md5(".".join(feature_values))),
                }
            )

        # Note - track_* is not used here as that often contains author version
        #  and we want to collide on different author versions
        # unique result
        unique_result = uid(
            sha256_author_action_uid,
            _remove_invalid_chars(event["security"]),
        )

        # create binary parent
        sha256 = encoded_event["sha256"]
        encoded_event.update(
            **{
                "_id": unique_result,
                "binary_info": {"name": "metadata", "parent": sha256},
                "security": event["security"],
                "encoded_security": event["encoded_security"],
                "timestamp": to_utc(event["timestamp"]),
                "action": event["action"],
                "author": event_author,
                "track_author": event["track_authors"][-1],
            }
        )

        if encoded_event.get("datastreams", []):
            # Note this relies on datastreams being sorted which it is earlier.
            sha256s = [x.get("sha256", "") for x in encoded_event["datastreams"]]
            encoded_event["uniq_data"] = uid(sha256_author_action_uid, md5(".".join(sha256s)))

        if encoded_event["action"] == azm.BinaryAction.Enriched:
            # drop 'entity.datastreams' in enriched events as it cannot be different to what is already seen
            encoded_event.pop("datastreams", None)
            encoded_event.pop("uniq_data", None)

        if encoded_event["action"] == azm.BinaryAction.Augmented:
            # drop content entries as they are already present in sourced or extracted
            datastreams = []
            for ds in encoded_event.get("datastreams", []):
                if ds["label"] != azm.DataLabel.CONTENT:
                    datastreams.append(ds)
            encoded_event["datastreams"] = datastreams

        # unique submission
        unique_submission = uid(
            # inherits uniqueness from result
            unique_result,
            event["track_source_references"],
            _remove_invalid_chars(event_source["timestamp"]),
            _remove_invalid_chars(event_source["security"]),
        )

        # sourced needs to track submission for obvious reasons
        # extracted & mapped need to track submission to shortcut a large number of queries
        # augmented needs to track submission to know what source a datastream is stored in
        # only enriched doesnt track submission
        if event["action"] != azm.BinaryAction.Enriched:
            # metadata also needs to track submission
            encoded_event.update(cls._encode_submission(event, unique_submission))

        if event["action"] in [azm.BinaryAction.Extracted, azm.BinaryAction.Mapped]:
            # mapped events can be source-level so may have a path of 1
            if len(event_source_path) >= 2:
                # unique link
                parent = event_source_path[-2]
                unique_link = uid(
                    # inherits uniqueness from submission and result
                    unique_submission,
                    parent["sha256"],
                )
                # metadata also needs to track link to parent
                encoded_event.update(cls._encode_link(event, unique_link))

        cls._apply_event_overrides(encoded_event, sha256)

        return encoded_event

    @classmethod
    def _apply_event_overrides(cls, event: dict, sha256) -> None:
        """Mutate the input event overriding opensearch fields for routing and indexing."""
        # first letter of id
        index_extension = f".{sha256[0]}"

        overrides = {
            "_binary_index": True,
            "_routing": sha256,
            "_index_extension": index_extension,
        }
        event.update(overrides)

    @classmethod
    def _generate_parent(cls, event: dict) -> dict:
        """Generate the parent event for an event."""
        sha256 = event["sha256"]
        parent = {
            # prevent sightings of binary from recreating parent binary doc again
            "_id": sha256,
            "binary_info": {"name": "binary"},
            # The parent doc does not need to store sha256 as a searchable property
            # as you can just look at the children docs for the sha256
            # either via has_child query or children aggregation.
            # Alternatively you can query by document id (which is the sha256).
        }
        # Everyone can read parent documents and security must be applied via child docs.
        # Easiest means is to perform a has_child or children aggregation.
        parent["security"] = "s-any"
        cls._encode_security(parent)
        cls._apply_event_overrides(parent, sha256)
        return parent

    @classmethod
    def filter_seen_and_create_parent_events(cls, event: dict) -> list[dict]:
        """Generate parent events for events and generate the parent events for the supplied events.

        Filter, works by filtering documents based on generated IDs.
        This increases performance due to expected frequent collisions.
        """
        parent_event = cls._generate_parent(event)

        # skip mapping docs when checking cache as unlikely to see again.
        # mapping docs are high frequency and don't have datastreams so other plugins won't run.
        if event["action"] in [azm.BinaryAction.Mapped]:
            return [parent_event, event]

        ret = []
        # check for cached items
        for i, evt in enumerate([parent_event, event]):
            if i == 0:
                # Parent event doesn't have any implications to the plugin version.
                basic = evt["_id"]
            else:
                # Normal binary enrichment event will be different depending on the author.
                # Append the author version to ensure that new versions overwrite old features, etc
                basic = evt["_id"] + "." + event["author"].get("version", "")
            # fast hash the id to use less ram
            hashed = xxhash.xxh3_128_digest(basic)
            if hashed not in cache_ids:
                # add id to cache
                cache_ids[hashed] = None
                # Add event to return values
                ret.append(evt)

        # return docs we haven't generated recently
        return ret

    @classmethod
    def recover_source_binary_node(cls, event: dict) -> dict:
        """Rebuild a child node for source path of source-binary submission.

        Cannot recover language field, but language on nodes is unused.
        """
        ret = {}
        for k in fields_recover_source_binary_node:
            if k in event:
                ret[k] = event[k]
        if "parent_relationship" in ret:
            ret["relationship"] = ret.pop("parent_relationship")

        return ret

    @classmethod
    def decode(cls, event: dict) -> dict:
        """Best effort decode, reconstructs the dispatcher binary event.

        * source.path: limited recall
        * track_link: limited recall
        * track_author: limited recall
        """
        if "track_author" in event:
            event["track_authors"] = [event.pop("track_author")]

        # rebuild source as best we can
        if "source" in event:
            event.pop("depth")
            # add child node to source path
            event["source"]["path"] = [cls.recover_source_binary_node(event)]
            event.pop("parent_relationship", None)
            # submission is always present if link is present
            if "parent" in event:
                # add parent author above child author
                # we don't know the parents relationship to other ancestors
                event["track_authors"].insert(0, event.pop("parent_track_author"))
                event["track_links"] = [event.pop("track_link")]
                event["source"]["path"].insert(0, event.pop("parent"))

        # rebuild entity if some info is present
        entity_props = [
            "sha256",
            "sha512",
            "sha1",
            "md5",
            "ssdeep",
            "encoded_ssdeep",
            "tlsh",
            "tlsh_vector",
            "mime",
            "magic",
            "file_format",
            "file_format_legacy",
            "file_extension",
            "size",
            "datastreams",
            "features",
            "info",
        ]
        event["entity"] = {}
        for k in entity_props:
            if k in event:
                event["entity"][k] = event.pop(k)
        if not event["entity"]:
            event.pop("entity")

        # remove enriched root properties
        root_props = [
            "features_map",
            "filename",
            "num_feature_names",
            "num_feature_values",
            "sha256_author_action",
            "uniq_features",
            "uniq_info",
            "uniq_data",
        ]
        for k in list(event.keys()):
            if k in root_props:
                event.pop(k)

        event.pop("binary_info", None)

        event["model_version"] = azm.CURRENT_MODEL_VERSION

        cls._decode_security(event)
        event.pop("security", None)
        event.pop("encoded", None)
        event.get("source", {}).pop("encoded_references", None)
        event.get("source", {}).pop("encoded_settings", None)
        event.get("entity", {}).pop("encoded_ssdeep", None)
        event.get("entity", {}).pop("tlsh_vector", None)
        for feat in event.get("source", {}).get("path", []):
            feat.pop("encoded", None)
        for feat in event.get("entity", {}).get("features", []):
            feat.pop("encoded", None)
            feat.pop("enriched", None)

        return event

    @classmethod
    def get_mapping_with_features(cls, features: list[str]) -> dict:
        """Update the mapping with additional feature names.

        This doesn't need to add historical feature names added previously, as opensearch combines them.
        """
        mp = copy.copy(cls.mapping)
        featmap = mp["properties"]["features_map"]["properties"]
        for feat in features:
            featmap.update({feat: {"type": "keyword"}})
        return mp
