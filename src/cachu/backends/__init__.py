"""Cache backend implementations.
"""
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import Any

NO_VALUE = object()


class Backend(ABC):
    """Abstract base class for cache backends.
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
    async def keys(self, pattern: str | None = None) -> AsyncIterator[str]:
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


__all__ = ['Backend', 'AsyncBackend', 'NO_VALUE']
