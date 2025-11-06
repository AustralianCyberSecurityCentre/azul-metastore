"""Encoder for status data."""

from azul_metastore import settings
from azul_metastore.common.utils import azsec
from azul_metastore.encoders.base_encoder import uid

from . import base_encoder


class Status(base_encoder.BaseIndexEncoder):
    """Converter for a status."""

    docname = "status"
    mapping = {
        "dynamic": "strict",
        "properties": {
            "security": {"type": "keyword"},
            "encoded_security": base_encoder.get_security_mapping(),
            "timestamp": {"type": "date"},
            "model_version": {"type": "integer"},
            "author": {
                "properties": {
                    "security": {"type": "keyword"},
                    "category": {"type": "keyword"},
                    "name": {"type": "keyword"},
                    "version": {"type": "keyword"},
                },
                "type": "object",
            },
            "entity": {
                "type": "object",
                "properties": {
                    "status": {"type": "keyword"},
                    "error": {"type": "keyword", "ignore_above": 1000},
                    "message": {"type": "keyword", "ignore_above": 1000},
                    "runtime": {"type": "double"},
                    "input": {
                        "type": "object",
                        "properties": {
                            "entity": {
                                "type": "object",
                                "properties": {
                                    "sha256": {"type": "keyword"},
                                },
                            },
                        },
                    },
                },
            },
            "encoded": {
                "type": "object",
                "properties": {
                    "author": {"eager_global_ordinals": True, "type": "keyword"},
                },
            },
        },
    }

    @classmethod
    def encode(cls, event: dict) -> dict:
        """Encode to opensearch layer format.

        Does not perform normalisation, that should occur as part of the models/basic_events.py.
        """
        # since status events must pass through the dispatcher, we never generate an id
        event["_id"] = event.pop("kafka_key")
        event["_index_extension"] = cls._categorise(event)

        event["security"] = azsec().string_combine(
            [
                event["author"]["security"],
                # note - source info is dropped so this can't be recomputed
                event["entity"]["input"]["source"]["security"],
                *(x["author"]["security"] for x in event["entity"]["input"]["source"]["path"]),
            ]
        )
        cls._encode_security(event)

        # drop data from origin input event that we don't care about
        # to reduce document size
        event["entity"]["input"].pop("author", None)
        event["entity"].pop("results", None)

        # drop a bunch of input properties
        keep_attrs = {"entity"}
        for k in list(event["entity"]["input"].keys()):
            if k not in keep_attrs:
                event["entity"]["input"].pop(k, None)

        # drop a bunch of entity properties
        keep_attrs = {"sha256"}
        for k in list(event["entity"]["input"]["entity"].keys()):
            if k not in keep_attrs:
                event["entity"]["input"]["entity"].pop(k, None)

        event["encoded"] = {
            "author": uid(event["author"]["name"], event["author"]["category"], event["author"].get("version", "")),
        }

        return event

    @classmethod
    def decode(cls, event: dict):
        """Decode to comms layer format."""
        cls._decode_security(event)
        event.pop("encoded")  # remove search enhancements
        return event

    @classmethod
    def _categorise(cls, doc: dict) -> str:
        """Return index extension for current doc."""
        s = settings.get()
        timestamp = doc["timestamp"]
        fmt = base_encoder.partition_format(timestamp, s.status_partition_unit)
        ret = ["", fmt]
        return ".".join(ret)
