"""Entity tag encoder."""

from __future__ import annotations

import re

from azul_metastore.common.utils import md5, to_utc

from . import base_encoder

safe_tag = re.compile(r"[a-z0-9\-]*$")


class InvalidAnnotation(Exception):
    """The supplied annotation is not valid."""

    pass


class Annotation(base_encoder.BaseIndexEncoder):
    """Converter for a tag referencing an entity."""

    docname = "annotation"
    mapping = {
        "dynamic": "strict",
        "properties": {
            "security": {"type": "keyword"},
            "encoded_security": base_encoder.get_security_mapping(),
            "timestamp": {"type": "date"},
            "owner": {"type": "keyword"},
            # types are currently: entity_tag, fv_tag
            "type": {"type": "keyword"},
            "sha256": {"type": "keyword"},
            "feature_name": {"type": "keyword"},
            "feature_value": {"type": "keyword"},
            "pivot": {"type": "keyword"},
            # optional
            "tag": {"type": "keyword"},
            "comment": {"type": "keyword"},
            "state": {"type": "keyword"},
        },
    }

    @classmethod
    def encode(cls, event: dict) -> dict:
        """Encode to opensearch layer format."""
        if "sha256" in event:
            event["sha256"] = event["sha256"].lower()
        if event["type"] not in ["fv_tag", "entity_tag"]:
            raise InvalidAnnotation(f"unknown annotation {event['type']}")
        if not safe_tag.match(event.get("tag", "")):
            raise InvalidAnnotation(f"bad characters in tag: {event['tag']}")
        if len(event.get("tag", "")) > 25:
            raise InvalidAnnotation(f'tag too long: {event["tag"]}')
        if len(event.get("comment", "")) > 1000:
            raise InvalidAnnotation(f'comment too long: {event["comment"]}')

        event["timestamp"] = to_utc(event["timestamp"])
        cls._encode_security(event)
        if "sha256" in event:
            f2 = event.get("sha256")
            event["pivot"] = f2
        else:
            f1 = event.get("feature_name")
            f2 = event.get("feature_value")
            event["pivot"] = base_encoder.uid(f1, f2)
        event["_id"] = base_encoder.uid(
            event["owner"], event["type"], event.get("tag") or md5(event.get("comment", "")), event["pivot"]
        )
        return event

    @classmethod
    def decode(cls, event: dict):
        """Decode to comms layer format."""
        cls._decode_security(event)
        event.pop("pivot", None)
        return event
