"""Encoder for plugin data."""

from azul_metastore.common.utils import azsec, to_utc

from . import base_encoder


class Plugin(base_encoder.BaseIndexEncoder):
    """Converter for a plugin registration event."""

    docname = "plugin"
    mapping = {
        "dynamic": "strict",
        "properties": {
            "security": {"type": "keyword"},
            "encoded_security": base_encoder.get_security_mapping(),
            "model_version": {"type": "integer"},
            "timestamp": {"type": "date"},
            "author": {
                "type": "object",
                "properties": {
                    "category": {"type": "keyword"},
                    "name": {"type": "keyword"},
                    "version": {"type": "keyword"},
                    "security": {"type": "keyword"},
                },
            },
            "entity": {
                "type": "object",
                "properties": {
                    "category": {"type": "keyword"},
                    "name": {"type": "keyword"},
                    "version": {"type": "keyword"},
                    "contact": {"type": "keyword"},
                    "description": {"type": "keyword"},
                    "features": {
                        "properties": {
                            "name": {"type": "keyword"},
                            "type": {"type": "keyword"},
                            "desc": {"type": "keyword"},
                            "tags": {"type": "keyword"},
                        },
                        "type": "object",
                    },
                    "config": {"enabled": False, "type": "object"},
                    "security": {"type": "keyword"},
                },
            },
        },
    }

    @classmethod
    def encode(cls, event: dict) -> dict:
        """Encode to opensearch layer format.

        Does not perform normalisation, that should occur as part of the models/basic_events.py.
        """
        # since plugin events must pass through the dispatcher, we never generate an id
        event["_id"] = event.pop("kafka_key")

        event["security"] = azsec().string_combine(
            [
                event["author"]["security"],
                event["entity"]["security"],
            ]
        )
        cls._encode_security(event)

        event["timestamp"] = to_utc(event["timestamp"])
        event.pop("flags", None)
        return event

    @classmethod
    def decode(cls, event: dict):
        """Decode to comms layer format."""
        cls._decode_security(event)
        return event
