"""Cache management for metastore.

Mostly for allowing unit tests to easily clear all cached results.
"""

import cachetools

caches = {}


def get_ttl_cache(name: str, **overrides) -> cachetools.TTLCache:
    """Return a new ttl cache."""
    kwargs = dict(maxsize=100, ttl=60)
    kwargs.update(overrides or {})
    _id = f"ttl-{name}"
    if _id in caches:
        raise Exception(f"cache already exists {_id}")
    caches[_id] = cachetools.TTLCache(**kwargs)
    return caches[_id]


def get_lru_cache(name: str, **overrides) -> cachetools.LRUCache:
    """Return a new lru cache."""
    kwargs = dict(maxsize=100)
    kwargs.update(overrides or {})
    _id = f"lru-{name}"
    if _id in caches:
        raise Exception(f"cache already exists {_id}")
    caches[_id] = cachetools.LRUCache(**kwargs)
    return caches[_id]


def clear():
    """Clear all cache objects of data."""
    for v in caches.values():
        if not v:
            # null cache
            continue
        v.clear()
