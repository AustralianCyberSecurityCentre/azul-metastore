"""Handles conversion of data to/from opensearch optimised format."""

from __future__ import annotations

import copy
import math
from typing import ClassVar

import pendulum
from azul_bedrock import models_settings
from azul_security import security as azul_security

from azul_metastore import settings
from azul_metastore.common import utils, wrapper

INCLUSIVE = azul_security.INCLUSIVE
EXCLUSIVE = azul_security.EXCLUSIVE
MARKINGS = azul_security.MARKINGS
S_ANY = "s-any"  # magic value for default accessibility (should NOT be a real marking)


def get_security_mapping() -> dict:
    """Get opensearch mapping of security."""
    ret = {
        # derived information that can be easily removed on read
        "type": "object",
        "properties": {
            "num_exclusive": {"type": "integer"},
            # inclusive and exclusive security, but encoded for dls
            INCLUSIVE: {"type": "keyword"},
            EXCLUSIVE: {"type": "keyword"},
            MARKINGS: {"type": "keyword"},
        },
    }
    return ret


def uid(*args) -> str:
    """Join a bunch of strings together, to form a unique identifier."""
    return ".".join(str(x if x is not None else "") for x in args)


def partition_format(timestamp: str, index_time_unit: models_settings.PartitionUnitEnum):
    """Calculate special timestamp string."""
    ts = pendulum.parse(timestamp)
    if isinstance(ts, (pendulum.Duration, pendulum.Time)):
        raise Exception(f"invalid timestamp {timestamp}; is not an absolute timestamp")

    if index_time_unit == models_settings.PartitionUnitEnum.year:
        ret = ts.format("YYYY")
    elif index_time_unit == models_settings.PartitionUnitEnum.month:
        ret = ts.format("YYYY-MM")
    elif index_time_unit == models_settings.PartitionUnitEnum.week:
        # ts.week_of_year gives odd results, e.g. 2019-12-31 -> 1
        ret = f'{ts.format("YYYY-MM")}-w{math.ceil(ts.day_of_year / 7):02}'
    elif index_time_unit == models_settings.PartitionUnitEnum.day:
        ret = ts.format("YYYY-MM-DD")
    elif index_time_unit == models_settings.PartitionUnitEnum.all:
        ret = "all"
    else:
        avail_options = ", ".join(models_settings.PartitionUnitEnum._member_names_)
        raise Exception(f"unknown value {index_time_unit=}, should be one of {avail_options}")
    return ret


class BaseIndexControl:
    """Shared functionality for handling documents stored in specific patterns of opensearch indices."""

    # the document name used to construct the indices, alias and template for this object
    docname: ClassVar[str]
    mapping: dict
    # datestring for version
    # changing the version will require manual actions from sysadmins such as incrementing the ingest prefix
    template_version: int = 20250519
    index_settings: dict = {
        "number_of_shards": 3,
        "number_of_replicas": 2,
        "refresh_interval": "5s",
    }
    w: wrapper.Wrapper

    def __init__(self, *, setting_overrides: dict = None) -> None:
        # If settings overrides are provided, apply the override.
        if setting_overrides:
            self.index_settings: dict = copy.deepcopy(self.index_settings)
            self.index_settings.update(setting_overrides)
        if not self.docname or self.mapping is None:
            raise NotImplementedError("encoder not fully defined")

        s = settings.get()

        self.w = wrapper.Wrapper(
            partition=s.partition,
            docname=self.docname,
            index_settings=self.index_settings,
            minimum_required_access=frozenset(
                x for x in (*utils.azsec().unsafe_to_safe(utils.azsec().minimum_required_access), S_ANY)
            ),
            mapping=self.mapping,
            version=self.template_version,
        )

    @classmethod
    def _encode_security(cls, d: dict) -> None:
        """Transform security object for storage in opensearch.

        :param d: Dictionary to be saved, with a key 'security'
        """
        azsec = utils.azsec()

        # convert security labels to valid roles
        # require at least one entry, so default to S_ANY
        sec = d.get("security")
        if not sec:
            # set default security
            sec = d["security"] = azsec.get_default_security()
        if sec == S_ANY:
            # anyone can access this document
            d["encoded_security"] = {EXCLUSIVE: [S_ANY], INCLUSIVE: [S_ANY], MARKINGS: []}
        else:
            parsed = azsec.string_parse(sec)
            d["encoded_security"] = {
                EXCLUSIVE: sorted(azsec.unsafe_to_safe(parsed.exclusive)) or [S_ANY],
                INCLUSIVE: sorted(azsec.unsafe_to_safe(parsed.inclusive)) or [S_ANY],
                MARKINGS: sorted(azsec.unsafe_to_safe(parsed.markings)) or [S_ANY],
            }

        # count number of exclusives
        d["encoded_security"]["num_exclusive"] = len(d["encoded_security"][EXCLUSIVE])

    @classmethod
    def _decode_security(cls, d: dict) -> None:
        """Transform security objects for return to user."""
        # encoded_security should be removed
        d.pop("encoded_security", None)


class BaseIndexEncoder(BaseIndexControl):
    """Shared functionality for encoding any events for Opensearch, while retaining original event structure."""

    @classmethod
    def encode(cls, event: dict) -> dict:
        """Encode to opensearch layer format.

        Does not perform normalisation, that should occur as part of the models/basic_events.py.
        """
        raise NotImplementedError()

    @classmethod
    def decode(cls, event: dict) -> dict:
        """Decode to comms layer format."""
        raise NotImplementedError()
