"""Provides models for binary/status/plugin events.

FUTURE don't add classmethods on to pydantic classes
"""

import json

from azul_bedrock import models_network as azm

from azul_metastore.common.utils import PreprocessException, azsec, jsondict


def normalise_security(d: dict) -> None:
    """Normalise an embedded security string or assign default value."""
    d["security"] = azsec().string_normalise(d.get("security") or azsec().get_default_security())


class BinaryEvent(azm.BinaryEvent):
    """A binary event."""

    @classmethod
    def normalise(cls, ev: azm.BinaryEvent) -> dict:
        """Cleanup strange event structure and verify it meets model."""
        feature_limit = 10000
        stream_limit = 100

        # limit num streams
        len_streams = len(ev.entity.datastreams)
        if len_streams > stream_limit:
            raise PreprocessException(f"too many streams: {len_streams} > {stream_limit}")

        # limit num features
        features = ev.entity.features
        len_features = len(features)
        if len_features > feature_limit:
            raise PreprocessException(f"too many features: {len_features} > {feature_limit}")

        # normalise entity id and hashes
        ev.entity.sha256 = ev.entity.sha256.lower() if ev.entity.sha256 else None
        ev.entity.sha512 = ev.entity.sha512.lower() if ev.entity.sha512 else None
        ev.entity.sha1 = ev.entity.sha1.lower() if ev.entity.sha1 else None
        ev.entity.md5 = ev.entity.md5.lower() if ev.entity.md5 else None
        for node in ev.source.path:
            node.sha256 = node.sha256.lower()

        # convert to dict
        dumped = jsondict(ev)

        # normalise security information from source, author and source path
        normalise_security(dumped["author"])
        normalise_security(dumped["source"])
        for node in dumped["source"]["path"]:
            normalise_security(node["author"])

        return dumped


class PluginEvent(azm.PluginEvent):
    """Author event as received from dispatcher."""

    @classmethod
    def normalise(cls, ev: azm.PluginEvent) -> dict:
        """Cleanup strange stuff that dispatcher does or did historically and verify it meets model."""
        # convert to dict
        dumped = jsondict(ev)

        normalise_security(dumped["author"])
        normalise_security(dumped["entity"])

        # ensure that supplied config has json strings for values
        for k, v in dumped["entity"].get("config", {}).items():
            try:
                json.loads(v)
            except json.decoder.JSONDecodeError as e:
                # raise error on invalid config
                raise Exception(f"plugin config value for '{k}' is not json string: {v}") from e

        return dumped


class StatusEvent(azm.StatusEvent):
    """Status event as received from dispatcher."""

    @classmethod
    def normalise(cls, ev: azm.StatusEvent) -> dict:
        """Cleanup strange stuff that dispatcher does or did historically and verify it meets model."""
        # convert to dict
        dumped = jsondict(ev)

        normalise_security(dumped["author"])
        normalise_security(dumped["entity"]["input"]["source"])
        for node in dumped["entity"]["input"]["source"]["path"]:
            normalise_security(node["author"])

        doc = dumped["entity"]["input"]
        # retries shouldn't be mapped by opensearch.
        doc.pop("retries", None)

        return dumped
