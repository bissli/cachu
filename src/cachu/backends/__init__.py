"""Cache backend implementations.
"""
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from ..mutex import AsyncCacheMutex, CacheMutex

NO_VALUE = object()


class Backend(ABC):
    """Abstract base class for cache backends with both sync and async interfaces.
    """

    # ===== Sync interface =====

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

    # ===== Stats interface (sync) =====

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

    # ===== Async interface =====

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

    # ===== Stats interface (async) =====

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

    # ===== Lifecycle =====

    def close(self) -> None:
        """Close the backend and release resources.
        """

    async def aclose(self) -> None:
        """Async close the backend and release resources.
        """


__all__ = ['Backend', 'NO_VALUE']
