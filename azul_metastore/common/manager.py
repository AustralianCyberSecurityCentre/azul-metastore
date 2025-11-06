"""Defines the manager class."""

from azul_security import security as azsec

from azul_metastore import settings
from azul_metastore.encoders import annotation, binary2, cache, plugin, status

from . import search_data, wrapper


class Manager:
    """Surround several Wrapper objects to expose access based on document type."""

    def __init__(self):
        """Manage multiple wrapper instances to allow interaction with documents stored in opensearch."""
        self.s = settings.get()

        self.azsec = azsec.Security()

        self.binary2 = binary2.Binary2()
        self.status = status.Status(setting_overrides=self.s.status_index_config.model_dump())
        self.plugin = plugin.Plugin(setting_overrides=self.s.plugin_index_config.model_dump())

        self.annotation = annotation.Annotation()
        self.cache = cache.Cache()

    def initialise(self, sd: search_data.SearchData, *, force: bool = False):
        """Make sure all indices and templates exist."""
        # create base indices first
        self.binary2.w.initialise(sd, force=force)
        self.status.w.initialise(sd, force=force)
        self.plugin.w.initialise(sd, force=force)
        self.annotation.w.initialise(sd, force=force)
        self.cache.w.initialise(sd, force=force)

        # set up required sub-indices for binary event sources
        for key, data in self.s.sources.items():
            # create aliases for source
            if data.elastic:
                # custom index settings are usually only used because features/info take up so much disk space
                wrapper.set_index_properties(self.s.partition, "binary2." + key, data.elastic)

    def check_canary(self, sd: search_data.SearchData):
        """Write a canary file to opensearch, to check that indices are available.

        Reuse the cache index, since there is no need for a separate index.
        """
        tag = {
            "type": "canary",
            "timestamp": "1993-12-10T00:00:01Z",
            "unique": "doom",
            "user_security": "guy",
            "count": "1",
        }
        self.cache.w.wrap_and_index_docs(sd, [self.cache.encode(tag)])

    def get_source_aliases(self, source: str) -> list[str]:
        """Return the source alias which can be used to query only documents for a specific source."""
        return [
            self.binary2.w.get_subalias(source),
        ]
