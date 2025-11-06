"""Settings classes using pydantic environment parsing."""

import datetime
import logging
import logging.handlers
import sys
from functools import cached_property

import cachetools
from azul_bedrock import models_settings
from fastapi import Request
from pydantic import BaseModel, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pythonjsonlogger import json

from azul_metastore.common import memcache

logger = logging.getLogger(__name__)
loki_logger = None
printed = False


class ConfigException(Exception):
    """Problem with the configuration of Azul Metastore."""

    pass


class IndexSettings(BaseModel):
    """Settings that can be edited for custom settings of indexes."""

    number_of_shards: int = 3
    number_of_replicas: int = 2
    refresh_interval: str = "5s"


class Metastore(BaseSettings):
    """Metastore specific environment variables parsed into settings object."""

    def __init__(self):
        """Init function."""
        global printed
        global loki_logger
        super().__init__()

        # use log level settings
        logger = logging.getLogger("azul_metastore")

        log_level = {
            "FATAL": logging.FATAL,
            "ERROR": logging.ERROR,
            "WARNING": logging.WARNING,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG,
        }[self.log_level]
        logger.setLevel(log_level)

        if not logger.hasHandlers():
            h = logging.StreamHandler(sys.stdout)
            h.setLevel(log_level)
            log_format = logging.Formatter(
                fmt="%(levelname)s\t%(asctime)s - %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S%z"
            )
            h.setFormatter(log_format)
            logger.addHandler(h)

            # Logging handler to log to file for Loki to collect for audit purposes.
            if self.error_log_file_path:
                fh = logging.handlers.TimedRotatingFileHandler(
                    self.error_log_file_path, backupCount=self.error_log_days_to_retain, when="D"
                )
                fh.setLevel(logging.ERROR)
                # Allow for setting extra parameters in errors as json to make them easier to parse in loki.
                # Recommend to set "error_type" and then whatever other fields are needed as so:
                # extra={"error_type": "feature_encoding", "author": author, "error": str(e)},
                error_formatter = json.JsonFormatter(timestamp="timestamp")
                error_formatter.datefmt = "%Y-%m-%d %H:%M:%S%z"
                fh.setFormatter(error_formatter)
                logger.addHandler(fh)

        # onetime setup of the global loki_logger
        if not loki_logger and self.special_log_file_path:
            # Add special suffix to allow for filtering out of special message types.
            loki_logger = logging.getLogger("azul_metastore_loki")
            loki_logger.setLevel(logging.INFO)
            special_log_format = logging.Formatter(fmt=self.special_log_outer_format, datefmt=r"%Y-%m-%dT%H:%M:%S")
            fh = logging.handlers.TimedRotatingFileHandler(
                self.special_log_file_path, backupCount=self.special_log_file_days_to_retain, when="D"
            )
            fh.setLevel(logging.INFO)
            fh.setFormatter(special_log_format)
            loki_logger.addHandler(fh)

        # prevent duplicate printing for each read of settings
        if not printed:
            if not self.opensearch_url:
                logger.warning("no opensearch url set for metastore!")
            if not self.certificate_verification:
                logger.warning("certificate verification disabled!")

            if self.opensearch_url.startswith("http:"):
                logger.warning(f"host not under ssl! {self.opensearch_url}")
            printed = True

        if not self.partition:
            raise ConfigException("metastore_partition must be set. Recommended to set to dev01, qa01, prod01, etc.")

    # location of opensearch cluster that can be queried
    # can also be a load balancer
    opensearch_url: str = ""

    # credentials for account to manage Azul indices in opensearch
    opensearch_username: str = "azul_writer"
    opensearch_password: str = ""  # noqa: S105 # nosec S105

    # admin credentials to create roles and rolemappings (must be used in conjunction with no-input flag)
    opensearch_admin_username: str = "azul_admin"
    opensearch_admin_password: str = ""  # noqa: S105 # nosec S105

    # azul.<partition> prefix for all indices in opensearch
    # to simplify migration recommended to set to dev01, qa01, prod01
    partition: str = "dev01"
    # if this value is incremented, all events will be reindexed from dispatcher
    # this is useful if moving between opensearch clusters or if mapping has changed
    ingestor_version_suffix: int = 0
    # intended for local testing only
    certificate_verification: bool = True

    # separated to diagnose memory issues in dispatcher
    # dispatcher for event interaction
    dispatcher_events_url: str = ""
    # dispatcher for file data interaction
    dispatcher_streams_url: str = ""

    log_level: str = "INFO"
    log_opensearch_queries: bool = False

    # File to store error logs in.
    error_log_file_path: str = ""
    # 1 month by default.
    error_log_days_to_retain: int = 30

    # Format for the special logger, is a cutdown version of audit_format with method and path obscured
    # so it doesn't have to be filtered out when collecting stats.
    special_log_outer_format: str = (
        "level=%(levelname)s time=%(asctime)s.%(msecs)d name=%(name)s function=%(funcName)s %(message)s"
    )
    special_log_message_format: str = (
        'full_time="{time:%d/%b/%Y:%H:%M:%S.%f}" connection="{connection}" username="{username}"'
        ' special_method="{method}" special_path="{path}" sha256="{sha256}"'
    )
    # File to log special audit events to for loki collection.
    special_log_file_path: str = ""
    # 1 month by default
    special_log_file_days_to_retain: int = 30

    sources: dict[str, models_settings.Source] = {}

    # Opensearch status config override
    status_index_config: IndexSettings = IndexSettings()
    # status retention
    status_partition_unit: models_settings.PartitionUnitEnum = models_settings.PartitionUnitEnum.week
    # Valid values are <number> 'years'|'months'|'weeks'|'days' e.g 4 months.
    status_expire_events: str = "2 weeks"

    # Opensearch plugin index config override
    plugin_index_config: IndexSettings = IndexSettings()

    @computed_field
    @cached_property
    def status_expire_events_ms(self) -> int:
        """Returns the number of milliseconds until statuses should be aged off."""
        return models_settings.convert_string_to_duration_ms(self.status_expire_events)

    # do not use security plugin endpoints
    no_security_plugin_compatibility: bool = False

    # start the prometheus server on this port, during a command line execution. 0 to disable.
    prometheus_port: int = 8900

    # Bypass checking if user is admin
    admin_check_bypass: bool = False
    # Opensearch role associated with admins
    admin_role: str = "admin"
    # URL for AI string filter
    smart_string_filter_url: str = ""
    # destination folder for sha256s txt that need to be checked for usage after a deletion
    # ensure that this folder is persistent to avoid orphaned files (i.e. pvc not ephemeral)
    purge_sha256_folder: str = ""
    model_config = SettingsConfigDict(env_prefix="metastore_")

    # add diagnostics for binaries which have more than X events
    warn_on_event_count: int = 10_000

    # cache that prevents duplicate opensearch doc creation
    binary2_cache_count: int = 1_000_000  # number of ids to cache, approx 64 bytes per id

    def log_to_loki(self, username: str, request: Request, sha256: str | None):
        """Log important information to loki that wouldn't otherwise be captured."""
        if loki_logger:
            fmt_vars = dict(
                time=datetime.datetime.now(tz=datetime.timezone.utc),
                connection=request.headers.get("connection", "-"),
                username=username,
                method=request.method,
                path=request.url.path,
                headers=request.headers,
                sha256=sha256,
            )
            loki_logger.info(self.special_log_message_format.format(**fmt_vars))


@cachetools.cached(cache=memcache.get_lru_cache("settings"))
def get():
    """Return a cached copy of metastore settings."""
    return Metastore()


@cachetools.cached(cache=memcache.get_lru_cache("get_writer_creds"))
def get_writer_creds():
    """Return credentials for the writer."""
    s = get()
    return {
        "unique": "writer",
        "format": "basic",
        "username": s.opensearch_username,
        "password": s.opensearch_password,
    }


def check_source_exists(source: str):
    """Return true if source is defined in Azul 3."""
    s = get()
    return source in s.sources


class BadSourceRefsException(Exception):
    """Source references are invalid."""

    pass


def check_source_references(source: str, references: dict):
    """Check that supplied source references are valid."""
    s = get()
    keys = set(references.keys())
    source_requires = set(x.name for x in s.sources[source].references if x.required)
    source_allows = set(x.name for x in s.sources[source].references)
    fields_missing = source_requires.difference(keys)
    fields_extra = keys.difference(source_allows)
    if fields_missing:
        raise BadSourceRefsException(f"{source}: missing {fields_missing}")
    if fields_extra:
        raise BadSourceRefsException(f"{source}: extra {fields_extra}")
