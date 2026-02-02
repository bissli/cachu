"""Async cache backend abstract base class.
"""
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any


class AsyncBackend(ABC):
    """Abstract base class for async cache backends.
    """

    @abstractmethod
    async def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found.
        """

    @abstractmethod
    async def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """

    @abstractmethod
    async def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete value by key.
        """

    @abstractmethod
    async def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """

    @abstractmethod
    def keys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Iterate over keys matching pattern.
        """

    @abstractmethod
    async def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """

    @abstractmethod
    async def close(self) -> None:
        """Close the backend and release resources.
        """
