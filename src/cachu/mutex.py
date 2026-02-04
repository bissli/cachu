"""Mutex implementations for cache dogpile prevention.
"""
import asyncio
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar, Self

if TYPE_CHECKING:
    import redis
    import redis.asyncio as aioredis


class CacheMutex(ABC):
    """Abstract base class for synchronous cache mutexes.
    """

    @abstractmethod
    def acquire(self, timeout: float | None = None) -> bool:
        """Acquire the lock. Returns True if acquired, False on timeout.
        """

    @abstractmethod
    def release(self) -> None:
        """Release the lock.
        """

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, *args: object) -> None:
        self.release()


class AsyncCacheMutex(ABC):
    """Abstract base class for asynchronous cache mutexes.
    """

    @abstractmethod
    async def acquire(self, timeout: float | None = None) -> bool:
        """Acquire the lock. Returns True if acquired, False on timeout.
        """

    @abstractmethod
    async def release(self) -> None:
        """Release the lock.
        """

    async def __aenter__(self) -> Self:
        await self.acquire()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.release()


class NullMutex(CacheMutex):
    """No-op mutex for testing or when locking is not needed.
    """

    def acquire(self, timeout: float | None = None) -> bool:
        return True

    def release(self) -> None:
        pass


class NullAsyncMutex(AsyncCacheMutex):
    """No-op async mutex for testing or when locking is not needed.
    """

    async def acquire(self, timeout: float | None = None) -> bool:
        return True

    async def release(self) -> None:
        pass


class ThreadingMutex(CacheMutex):
    """Per-key threading.Lock for local dogpile prevention.
    """
    _locks: ClassVar[dict[str, threading.Lock]] = {}
    _registry_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self, key: str) -> None:
        self._key = key
        self._acquired = False
        with self._registry_lock:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            self._lock = self._locks[key]

    def acquire(self, timeout: float | None = None) -> bool:
        if timeout is None:
            self._acquired = self._lock.acquire()
        else:
            self._acquired = self._lock.acquire(timeout=timeout)
        return self._acquired

    def release(self) -> None:
        if self._acquired:
            self._lock.release()
            self._acquired = False

    @classmethod
    def clear_locks(cls) -> None:
        """Clear all locks. For testing only.
        """
        with cls._registry_lock:
            cls._locks.clear()


class AsyncioMutex(AsyncCacheMutex):
    """Per-key asyncio.Lock for local async dogpile prevention.
    """
    _locks: ClassVar[dict[str, asyncio.Lock]] = {}

    def __init__(self, key: str) -> None:
        self._key = key
        self._acquired = False
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        self._lock = self._locks[key]

    async def acquire(self, timeout: float | None = None) -> bool:
        if timeout is None:
            await self._lock.acquire()
            self._acquired = True
            return True

        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
            self._acquired = True
            return True
        except asyncio.TimeoutError:
            return False

    async def release(self) -> None:
        if self._acquired:
            self._lock.release()
            self._acquired = False

    @classmethod
    def clear_locks(cls) -> None:
        """Clear all locks. For testing only.
        """
        cls._locks.clear()


class RedisMutex(CacheMutex):
    """Distributed lock using Redis SET NX EX.
    """
    _RELEASE_SCRIPT = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    end
    return 0
    """

    def __init__(
        self,
        client: 'redis.Redis',
        key: str,
        lock_timeout: float = 10.0,
    ) -> None:
        self._client = client
        self._key = key
        self._lock_timeout = lock_timeout
        self._token = str(uuid.uuid4())
        self._acquired = False

    def acquire(self, timeout: float | None = None) -> bool:
        timeout = timeout or self._lock_timeout
        end = time.time() + timeout
        while time.time() < end:
            if self._client.set(
                self._key,
                self._token,
                nx=True,
                ex=int(self._lock_timeout),
            ):
                self._acquired = True
                return True
            time.sleep(0.05)
        return False

    def release(self) -> None:
        if self._acquired:
            self._client.eval(self._RELEASE_SCRIPT, 1, self._key, self._token)
            self._acquired = False


class AsyncRedisMutex(AsyncCacheMutex):
    """Async distributed lock using redis.asyncio.
    """
    _RELEASE_SCRIPT = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    end
    return 0
    """

    def __init__(
        self,
        client: 'aioredis.Redis',
        key: str,
        lock_timeout: float = 10.0,
    ) -> None:
        self._client = client
        self._key = key
        self._lock_timeout = lock_timeout
        self._token = str(uuid.uuid4())
        self._acquired = False

    async def acquire(self, timeout: float | None = None) -> bool:
        timeout = timeout or self._lock_timeout
        end = time.time() + timeout
        while time.time() < end:
            if await self._client.set(
                self._key,
                self._token,
                nx=True,
                ex=int(self._lock_timeout),
            ):
                self._acquired = True
                return True
            await asyncio.sleep(0.05)
        return False

    async def release(self) -> None:
        if self._acquired:
            await self._client.eval(self._RELEASE_SCRIPT, 1, self._key, self._token)
            self._acquired = False


__all__ = [
    'CacheMutex',
    'AsyncCacheMutex',
    'NullMutex',
    'NullAsyncMutex',
    'ThreadingMutex',
    'AsyncioMutex',
    'RedisMutex',
    'AsyncRedisMutex',
]
