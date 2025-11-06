"""Encoder for feature values information."""

from __future__ import annotations

from azul_metastore.common.utils import md5, to_utc

from . import base_encoder


class Cache(base_encoder.BaseIndexEncoder):
    """Precalculated count for feature value."""

    docname = "cache"
    mapping = {
        "dynamic": "strict",
        "properties": {
            "security": {"type": "keyword"},
            "encoded_security": base_encoder.get_security_mapping(),
            "timestamp": {"type": "date"},
            "type_unique": {"type": "keyword"},
            # category of this cached document
            "type": {"type": "keyword"},
            # unique marking within the category
            "unique": {"type": "keyword"},
            # describes string that must match when reading data to ensure correct format
            # only implemented for 'data' access
            "version": {"type": "keyword"},
            # total docs in opensearch when cached doc was created
            "docs": {"type": "long"},
            # result of count operation when cached doc was created
            "count": {"type": "long"},
            # true if count operation was accurate
            "accurate": {"type": "boolean"},
            # unique security marking for user that executed count
            "user_security": {"type": "keyword"},
            # abstract data attached to document, for non count operations
            "data": {"enabled": False},
        },
    }

    @classmethod
    def encode(cls, event: dict) -> dict:
        """Encode to opensearch layer format."""
        event["timestamp"] = to_utc(event["timestamp"])
        cls._encode_security(event)
        event["type_unique"] = md5(".".join([event["type"], event["unique"]]))
        event["_id"] = cls.calc_id(event["type"], event["unique"], event["user_security"])
        return event

    @classmethod
    def decode(cls, event: dict):
        """No need to do anything."""
        return event

    @classmethod
    def calc_id(cls, type, unique, user_security):
        """Calculate a unique cache id for user security level."""
        return ".".join([type, unique, user_security])
