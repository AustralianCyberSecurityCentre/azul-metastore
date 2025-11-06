"""Encoder for feature values information."""

from __future__ import annotations

import base64
import ipaddress
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class FeatureEncodeException(Exception):
    """Something went wrong while encoding features."""

    pass


def enrich_feature(feat: dict):
    """Add an 'enriched' dict to features with parsed integers, hostnames, ports, and other info."""
    try:
        # parse value into individual fields, returns error if something went wrong.
        # Error is returned as string to allow rest of this function to run.
        feat["enriched"] = _parse_feature_value(feat["value"], feat["type"])
    except Exception as e:
        raise FeatureEncodeException(f"failed to parse and enrich feature: {feat} with error message: {e}") from e


def _parse_feature_value(value: str, _type: str) -> dict:
    """Parses integers, hostnames, ports, and other info."""
    cases = {
        "integer": lambda v: {"integer": int(v)},
        "float": lambda v: {"float": float(v)},
        "string": lambda v: {},
        "binary": lambda v: {"binary_string": base64.b64decode(v).decode("utf-8", errors="ignore")},
        "datetime": lambda v: {"datetime": v},
        "filepath": _process_path,
        "uri": _process_uri,
    }

    if _type not in cases.keys():
        raise Exception(f"unhandled type: {_type} with value {value}")

    return cases[_type](value)


def _process_path(value) -> dict:
    """Try to normalise paths. cant use python for this, since it is dependant on platform the code is run on."""
    # FUTURE requires a reingest to change this to treat windows and unix paths differently
    # the original windows / unix path before normalisation is kept as the feature 'value'
    # and if we use '/' for folder separator here its easier to search across both windows and unix combined
    # if we find a windows style path separator first, change all separators to be unix type
    bs = value.find("\\")
    fs = value.find("/")
    if fs < 0 or 0 <= bs < fs:
        value = value.replace("\\", "/")

    return {"filepath": value}


def _process_uri(value) -> dict:
    """Parse uris into various components.

    Examples:
        http://myuser@blah.com:443/this/is/the/path.html?qry&morequer#fragmetnts
        http://myuser:password@blah.com:443/this/is/the/path.html?qry&morequer#fragmetnts
        https://blah.com/this/is/the/path.html
        http://blah.com/this/is/the/path.html
        http://blah.com
        http://201.111.20.5
        http://201.111.20.5/blah/file.txt
        http://myuser@blah.com:443/this/is/the/path.html?qry&morequer#fragmetnts
    """
    d = {}
    # handle cases where host details/endpoint sent without scheme prefix
    if "://" not in value and not value.lower().startswith(("mailto:", "http:", "https:")):
        value = "none://" + value
    r = urlparse(value)

    d["scheme"] = r.scheme if r.scheme != "none" else None
    d["netloc"] = r.netloc
    d["filepath"] = r.path
    d["params"] = r.params
    d["query"] = r.query
    d["fragment"] = r.fragment
    d["username"] = r.username
    d["password"] = r.password
    d["hostname"] = r.hostname
    d["port"] = int(r.port) if r.port else None

    # check if hostname is ip address
    try:
        ipaddress.ip_address(r.hostname)
    except ValueError:
        pass
    else:
        d["ip"] = r.hostname

    # exclude empty values
    return {k: v for k, v in d.items() if v}
