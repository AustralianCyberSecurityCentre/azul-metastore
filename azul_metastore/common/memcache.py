"""Cache management for metastore.

Mostly for allowing unit tests to easily clear all cached results.
"""

import cachetools
from azul_bedrock import exceptions_metastore
from azul_bedrock.exception_enums import ExceptionCodeEnum

caches = {}


def get_ttl_cache(name: str, maxsize: int = 100, ttl: int = 60) -> cachetools.TTLCache:
    """Return a new ttl cache."""
    _id = f"ttl-{name}"
    if _id in caches:
        raise exceptions_metastore.CacheAlreadyExistsException(
            ref=f"cache already exists {_id}",
            internal=ExceptionCodeEnum.MetastoreMemcacheTTLCacheAlreadyCreated,
            parameters={"cache_id": _id},
        )
    caches[_id] = cachetools.TTLCache(maxsize=maxsize, ttl=ttl)
    return caches[_id]


def get_lru_cache(name: str, maxsize: int = 100) -> cachetools.LRUCache:
    """Return a new lru cache."""
    _id = f"lru-{name}"
    if _id in caches:
        raise exceptions_metastore.CacheAlreadyExistsException(
            ref=f"cache already exists {_id}",
            internal=ExceptionCodeEnum.MetastoreMemcacheLRUCacheAlreadyCreated,
            parameters={"cache_id": _id},
        )
    caches[_id] = cachetools.LRUCache(maxsize=maxsize)
    return caches[_id]


def clear():
    """Clear all cache objects of data."""
    for v in caches.values():
        if not v:
            # null cache
            continue
        v.clear()
