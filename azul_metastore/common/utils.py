"""Common utilities."""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import time
from typing import Callable, Iterable

import cachetools
import pendulum
from azul_bedrock import models_network as azm
from azul_security import security as azul_security
from fastapi.encoders import jsonable_encoder
from prometheus_client import Counter
from pydantic import BaseModel

from azul_metastore.common import memcache
from azul_metastore.common.query_info import IngestError

prom_ingest = Counter("azul_ingest", "Ingestion events for Azul metadata", ["type", "status", "plugin"])
prom_duplicates = Counter(
    "azul_ingest_duplicates", "Number of dropped duplicate ingestion events for Azul metadata", ["type", "plugin"]
)
logger = logging.getLogger(__name__)


def jsondict(d: BaseModel) -> dict:
    """Return standardised python json dict from pydantic model."""
    return jsonable_encoder(d, exclude_defaults=True, exclude_unset=True)


class PreprocessException(Exception):
    """The object is unable to be encoded for opensearch."""

    pass


class BadSourceException(PreprocessException):
    """The supplied source was not specified in configuration."""

    pass


class NonUniqueDispatcherIdException(Exception):
    """Exception raised if a dispatcherId is not unique."""

    pass


def md5(text: str):
    """Return string md5 representing incoming text."""
    return hashlib.md5(text.encode()).hexdigest()  # noqa: S303 # nosec B303, B324


@cachetools.cached(cache=memcache.get_lru_cache("azsec"))
def azsec():
    """Return an initialised and cached security provider."""
    return azul_security.Security()


class Measurer:
    """Provides info on how long an operation takes."""

    def __init__(self, msg):
        """Initialise."""
        self.msg = msg

    def __enter__(self):
        """Enter."""
        self.start = time.perf_counter()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit."""
        self.end = time.perf_counter()
        elapsed = self.end - self.start
        logger.info(f"{self.msg}: elapsed {elapsed:.2f}s")


def to_utc(x: str) -> str:
    """Convert a iso 8601 string to equivalent in UTC."""
    return pendulum.parse(x).in_timezone(pendulum.UTC).to_iso8601_string()


def chunker(iterator: list, max_items: int = 100) -> Iterable[list]:
    """Return items from iterator in chunks of 100."""
    ret = []
    for item in iterator:
        ret.append(item)
        if len(ret) >= max_items:
            yield ret
            ret = []
    if ret:
        yield ret


def capture_write_stats(format: str):
    """Capture stats about event creation."""

    def _capture_write_stats(func: Callable[[], tuple[list[IngestError], list[azm.BaseEvent]]]):
        def _stats_inner(ctx, docs, *args, **kwargs) -> tuple[int, int]:
            bad_docs, duplicate_docs = func(ctx, docs, *args, **kwargs)

            if len(bad_docs) > len(docs):
                logger.info("ingestor - more bad docs than original docs")
            else:
                # count all the successful indexed docs.
                # FUTURE the ingestor splits docs into parts which breaks prom tracking
                prom_ingest.labels(type=format, status="success", plugin="any").inc(len(docs) - len(bad_docs))

            # count all errors
            for err in bad_docs:
                try:
                    info = copy.deepcopy(err.doc)
                    if not isinstance(info, dict):
                        info = jsondict(info)
                    if info.get("model", "") == azm.ModelType.Binary:
                        info.get("source", {}).pop("path", None)
                        info.get("entity", {}).pop("features", None)
                        info.get("entity", {}).pop("datastreams", None)
                        info.get("entity", {}).pop("info", None)
                    logger.error(
                        f"Event write failure (cutoff after 1000 chars) '{err.error_type}'\n"
                        f"{err.error_reason}\n{json.dumps(info)[:1000]}"
                    )
                    # record failure in prometheus
                    prom_ingest.labels(
                        type=format, status=err.error_type, plugin=info.get("author", {}).get("name", "unknown")
                    ).inc()
                except Exception as e:
                    logger.error(
                        f"Event write failure + error handle failure {str(e)}\n'{err.error_type}'\n"
                        f"{err.error_reason}\n{err.doc}"
                    )

            # Count all duplicate documents that were dropped.
            for duplicate_doc in duplicate_docs:
                try:
                    prom_duplicates.labels(type=format, plugin=duplicate_doc.author.name).inc()
                except Exception as e:
                    logger.error(f"Duplicate handle failure {str(e)}\n{duplicate_doc}")
                    continue
            return len(bad_docs), len(duplicate_docs)

        return _stats_inner

    return _capture_write_stats
