"""Type definitions for the cache library.
"""
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass
class CacheEntry:
    """Cache entry metadata passed to validate callbacks.
    """
    value: Any
    created_at: float
    age: float


@dataclass
class CacheInfo:
    """Cache statistics for a decorated function.
    """
    hits: int
    misses: int
    currsize: int


@dataclass
class CacheMeta:
    """Metadata attached to cached functions.
    """
    ttl: int
    backend: str
    tag: str
    exclude: set[str]
    cache_if: Callable[[Any], bool] | None
    validate: Callable[[CacheEntry], bool] | None
    package: str
    key_generator: Callable[..., str]
