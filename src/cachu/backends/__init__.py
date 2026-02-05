"""Cache backend implementations.
"""
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ..mutex import AsyncCacheMutex, CacheMutex

NO_VALUE = object()


class SyncBackend(ABC):
    """Sync-only cache backend interface.
    """

    @abstractmethod
    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found.
        """

    @abstractmethod
    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """

    @abstractmethod
    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete value by key.
        """

    @abstractmethod
    def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """

    @abstractmethod
    def keys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys matching pattern.
        """

    @abstractmethod
    def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """

    @abstractmethod
    def get_mutex(self, key: str) -> 'CacheMutex':
        """Get a mutex for dogpile prevention on the given key.
        """

    def close(self) -> None:
        """Close the backend and release resources.
        """


class AsyncBackend(ABC):
    """Async-only cache backend interface.
    """

    @abstractmethod
    async def aget(self, key: str) -> Any:
        """Async get value by key. Returns NO_VALUE if not found.
        """

    @abstractmethod
    async def aget_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Async get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """

    @abstractmethod
    async def aset(self, key: str, value: Any, ttl: int) -> None:
        """Async set value with TTL in seconds.
        """

    @abstractmethod
    async def adelete(self, key: str) -> None:
        """Async delete value by key.
        """

    @abstractmethod
    async def aclear(self, pattern: str | None = None) -> int:
        """Async clear entries matching pattern. Returns count of cleared entries.
        """

    @abstractmethod
    def akeys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Async iterate over keys matching pattern.
        """

    @abstractmethod
    async def acount(self, pattern: str | None = None) -> int:
        """Async count keys matching pattern.
        """

    @abstractmethod
    def get_async_mutex(self, key: str) -> 'AsyncCacheMutex':
        """Get an async mutex for dogpile prevention on the given key.
        """

    async def aclose(self) -> None:
        """Async close the backend and release resources.
        """


@runtime_checkable
class SupportsStats(Protocol):
    """Protocol for backends that track cache statistics (sync).
    """

    def incr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Increment a stat counter for a function.
        """
        ...

    def get_stats(self, fn_name: str) -> tuple[int, int]:
        """Get (hits, misses) for a function.
        """
        ...

    def clear_stats(self, fn_name: str | None = None) -> None:
        """Clear stats for a function, or all stats if fn_name is None.
        """
        ...


@runtime_checkable
class SupportsAsyncStats(Protocol):
    """Protocol for backends that track cache statistics (async).
    """

    async def aincr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Async increment a stat counter for a function.
        """
        ...

    async def aget_stats(self, fn_name: str) -> tuple[int, int]:
        """Async get (hits, misses) for a function.
        """
        ...

    async def aclear_stats(self, fn_name: str | None = None) -> None:
        """Async clear stats for a function, or all stats if fn_name is None.
        """
        ...


class Backend(SyncBackend, AsyncBackend):
    """Full backend supporting both sync and async operations.

    Concrete backends should also implement SupportsStats and SupportsAsyncStats
    for statistics tracking (duck typed via Protocol).
    """

    @abstractmethod
    def incr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Increment a stat counter for a function.
        """

    @abstractmethod
    def get_stats(self, fn_name: str) -> tuple[int, int]:
        """Get (hits, misses) for a function.
        """

    @abstractmethod
    def clear_stats(self, fn_name: str | None = None) -> None:
        """Clear stats for a function, or all stats if fn_name is None.
        """

    @abstractmethod
    async def aincr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Async increment a stat counter for a function.
        """

    @abstractmethod
    async def aget_stats(self, fn_name: str) -> tuple[int, int]:
        """Async get (hits, misses) for a function.
        """

    @abstractmethod
    async def aclear_stats(self, fn_name: str | None = None) -> None:
        """Async clear stats for a function, or all stats if fn_name is None.
        """


__all__ = [
    'AsyncBackend',
    'Backend',
    'NO_VALUE',
    'SupportsAsyncStats',
    'SupportsStats',
    'SyncBackend',
]
