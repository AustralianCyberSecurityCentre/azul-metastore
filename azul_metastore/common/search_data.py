"""Search data representing a specific users ability to access opensearch."""

from dataclasses import dataclass, field

import cachetools
import opensearchpy
from azul_bedrock.models_restapi.basic import QueryInfo

from azul_metastore import settings
from azul_metastore.common import memcache


class BadCredentialsException(Exception):
    """Supplied credentials were invalid."""

    pass


def credentials_to_access(c: dict) -> dict:
    """Return credentials in format used to get access."""
    access = {}
    format = c.get("format")
    try:
        if format == "basic":
            access["http_auth"] = (c["username"], c["password"])
        elif format == "jwt":
            access["headers"] = {"Authorization": c["token"]}
        elif format == "oauth":
            access["headers"] = {"Authorization": f'Bearer {c["token"]}'}
        else:
            raise BadCredentialsException(f'unrecognised credential format: {c["format"]}')
    except KeyError as e:
        raise BadCredentialsException(f"missing/bad parameter: {str(e)}") from e

    return access


@cachetools.cached(cache=memcache.get_ttl_cache("creds_es"), key=lambda x: x["unique"])
def credentials_to_es(c: dict) -> opensearchpy.OpenSearch:
    """Turn credentials into an opensearch object and cache. Invalidate after some time."""
    access = credentials_to_access(c)
    s = settings.get()
    # unfortunately opensearchpy does not use requests library, but urlli3 directly
    # we have to manually reference certificates not found in Certifi for consistency
    # as we query opensearch via both requests and opensearchpy
    if not s.certificate_verification:
        access["verify_certs"] = False
        access["ssl_show_warn"] = False
    access["timeout"] = 120

    # enable transport compression
    return opensearchpy.OpenSearch(hosts=s.opensearch_url, http_compress=True, **access)


@dataclass
class SearchData:
    """Everything needed to execute a search."""

    credentials: dict
    security_exclude: list[str]  # list of user-specified security exclusions to apply to documents.

    # log opensearch queries and responses to python logger
    enable_log_es_queries: bool = False
    # Store the run es queries so that they can be reviewed after being run.
    enable_capture_es_queries: bool = False
    captured_es_queries: list[QueryInfo] = field(default_factory=lambda: [])

    def access(self) -> dict:
        """Return access data."""
        return credentials_to_access(self.credentials)

    def es(self) -> opensearchpy.OpenSearch:
        """Opensearch object. Usually has some auth info loaded to allow authenticated queries."""
        # FUTURE create on init instead
        return credentials_to_es(self.credentials)

    def unique(self) -> str:
        """Return unique representation of users access."""
        return f"{self.credentials['unique']}|{'.'.join(sorted(self.security_exclude))}"

    def clear_state(self) -> str:
        """Clear any state on the SearchData, including logged opensearch queries."""
        self.captured_es_queries = []


def get_writer_search_data() -> SearchData:
    """Return search data for writer user."""
    return SearchData(credentials=settings.get_writer_creds(), security_exclude=[])
