"""Null cache backend for testing (passthrough, no caching).
"""
from collections.abc import AsyncIterator, Iterator
from typing import Any, Literal

from ..mutex import NullAsyncMutex, NullMutex
from . import NO_VALUE, Backend


class NullBackend(Backend):
    """Passthrough backend that never caches anything.

    Useful for testing and disabling caching per-function without
    changing code structure.
    """

    def get(self, key: str) -> Any:
        """Always returns NO_VALUE.
        """
        return NO_VALUE

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Always returns (NO_VALUE, None).
        """
        return NO_VALUE, None

    def set(self, key: str, value: Any, ttl: int) -> None:
        """No-op.
        """

    def delete(self, key: str) -> None:
        """No-op.
        """

    def clear(self, pattern: str | None = None) -> int:
        """No-op, returns 0.
        """
        return 0

    def keys(self, pattern: str | None = None) -> Iterator[str]:
        """Yields nothing.
        """
        return iter([])

    def count(self, pattern: str | None = None) -> int:
        """Always returns 0.
        """
        return 0

    def get_mutex(self, key: str) -> NullMutex:
        """Returns NullMutex (no-op mutex).
        """
        return NullMutex()

    def incr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """No-op.
        """

    def get_stats(self, fn_name: str) -> tuple[int, int]:
        """Always returns (0, 0).
        """
        return (0, 0)

    def clear_stats(self, fn_name: str | None = None) -> None:
        """No-op.
        """

    async def aget(self, key: str) -> Any:
        """Always returns NO_VALUE.
        """
        return NO_VALUE

    async def aget_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Always returns (NO_VALUE, None).
        """
        return NO_VALUE, None

    async def aset(self, key: str, value: Any, ttl: int) -> None:
        """No-op.
        """

    async def adelete(self, key: str) -> None:
        """No-op.
        """

    async def aclear(self, pattern: str | None = None) -> int:
        """No-op, returns 0.
        """
        return 0

    async def akeys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Yields nothing.
        """
        return
        yield  # type: ignore[misc]

    async def acount(self, pattern: str | None = None) -> int:
        """Always returns 0.
        """
        return 0

    def get_async_mutex(self, key: str) -> NullAsyncMutex:
        """Returns NullAsyncMutex (no-op async mutex).
        """
        return NullAsyncMutex()

    async def aincr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """No-op.
        """

    async def aget_stats(self, fn_name: str) -> tuple[int, int]:
        """Always returns (0, 0).
        """
        return (0, 0)

    async def aclear_stats(self, fn_name: str | None = None) -> None:
        """No-op.
        """
