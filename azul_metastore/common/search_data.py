"""Search data representing a specific users ability to access opensearch."""

from dataclasses import dataclass, field

import cachetools
import opensearchpy
from azul_bedrock.datastore import Credentials
from azul_bedrock.datastore import credentials_to_access as bed_credentials_to_access
from azul_bedrock.datastore import credentials_to_es as bed_credentials_to_es
from azul_bedrock.models_restapi.basic import QueryInfo

from azul_metastore import settings
from azul_metastore.common import memcache


@cachetools.cached(cache=memcache.get_ttl_cache("creds_es"), key=lambda x: x["unique"])
def credentials_to_es(c: Credentials) -> opensearchpy.OpenSearch:
    """Cache acquisition of opensearch credentials."""
    return bed_credentials_to_es(c)


@dataclass
class SearchData:
    """Everything needed to execute a search."""

    credentials: Credentials
    security_exclude: list[str]  # list of user-specified security exclusions to apply to documents.
    security_include: list[str]  # list of user-specified security included RELS to apply to documents using AND.
    security_filter: str | None = None
    # log opensearch queries and responses to python logger
    enable_log_es_queries: bool = False
    # Store the run es queries so that they can be reviewed after being run.
    enable_capture_es_queries: bool = False
    captured_es_queries: list[QueryInfo] = field(default_factory=lambda: [])

    def access(self) -> dict:
        """Return access data."""
        return bed_credentials_to_access(self.credentials)

    def es(self) -> opensearchpy.OpenSearch:
        """Opensearch object. Usually has some auth info loaded to allow authenticated queries."""
        # FUTURE create on init instead
        return credentials_to_es(self.credentials)

    def unique(self) -> str:
        """Return unique representation of users access."""
        return f"{self.credentials.unique}|{'.'.join(sorted(self.security_exclude))}"

    def clear_state(self):
        """Clear any state on the SearchData, including logged opensearch queries."""
        self.captured_es_queries = []


def get_writer_search_data() -> SearchData:
    """Return search data for writer user."""
    return SearchData(
        credentials=settings.get_writer_creds(),
        security_exclude=[],
        security_include=[],
    )
