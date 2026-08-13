"""Mutex implementations for cache dogpile prevention.
"""
import asyncio
import hashlib
import math
import threading
import time
import uuid
import weakref
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Self

if TYPE_CHECKING:
    import redis
    import redis.asyncio as aioredis

_MIN_WAIT = 0.001


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

    Notes
    -----
    - The registry holds locks weakly, so an entry disappears once no live
      mutex references it. A strong dict grew one entry per distinct cache
      key and never shrank, which reintroduced the unbounded per-key growth
      that `memory_maxsize` exists to stop: 200,000 caller-supplied keys
      cost tens of megabytes of locks whatever bound the cache itself had.
    - Every mutex that needs a key keeps a strong reference for its whole
      lifetime, so two callers contending on one key still share one lock;
      only genuinely unused entries are collected.
    """
    _locks: ClassVar['weakref.WeakValueDictionary[str, threading.Lock]'] = (
        weakref.WeakValueDictionary())
    _registry_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self, key: str) -> None:
        self._key = key
        self._acquired = False
        with self._registry_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            self._lock = lock

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
    """Per-key asyncio.Lock for async dogpile prevention, scoped per event loop.

    Notes
    -----
    - The outer registry is keyed weakly by event loop and the inner one
      weakly by lock, so neither a finished loop nor an idle key is
      retained. A strong inner dict grew one entry per distinct cache key
      for the life of the loop, defeating `memory_maxsize` for exactly the
      caller-influenced key spaces it is meant to bound.
    - A mutex holds its lock strongly from `acquire` until it is discarded,
      so contending callers still share one lock per key.
    """
    _loop_locks: ClassVar[
        'weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, '
        'weakref.WeakValueDictionary[str, asyncio.Lock]]'
    ] = weakref.WeakKeyDictionary()
    _registry_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self, key: str) -> None:
        self._key = key
        self._acquired = False
        self._lock: asyncio.Lock | None = None

    def _resolve_lock(self) -> asyncio.Lock:
        """Return the lock for this key on the currently running event loop.
        """
        loop = asyncio.get_running_loop()
        with self._registry_lock:
            per_loop = self._loop_locks.get(loop)
            if per_loop is None:
                per_loop = weakref.WeakValueDictionary()
                self._loop_locks[loop] = per_loop
            lock = per_loop.get(self._key)
            if lock is None:
                lock = asyncio.Lock()
                per_loop[self._key] = lock
            return lock

    async def acquire(self, timeout: float | None = None) -> bool:
        """Acquire the per-loop lock, waiting at most `timeout` seconds.

        Notes
        -----
        - A non-positive timeout is floored at `_MIN_WAIT` rather than
          passed through. `asyncio.wait_for(..., 0)` wraps the coroutine in
          a not-yet-done task and raises even on a free lock, so a zero wait
          would report contention that does not exist - reachable whenever
          `cache_deadline` clamps the dogpile wait to nothing.
        - Testing `locked()` instead is not sound either: `Lock.release()`
          clears the flag before waking the next waiter, so during that
          handoff a free-looking lock still has a queued owner and a bare
          `await acquire()` would block for that owner's whole critical
          section.
        - The floor is small enough to be indistinguishable from "try once"
          and bounded, which is the property the caller actually needs.
        """
        self._lock = self._resolve_lock()
        if timeout is None:
            await self._lock.acquire()
            self._acquired = True
            return True

        try:
            await asyncio.wait_for(
                self._lock.acquire(), timeout=max(timeout, _MIN_WAIT))
            self._acquired = True
            return True
        except asyncio.TimeoutError:
            return False

    async def release(self) -> None:
        if self._acquired and self._lock is not None:
            self._lock.release()
            self._acquired = False

    @classmethod
    def clear_locks(cls) -> None:
        """Clear all locks. For testing only.
        """
        with cls._registry_lock:
            cls._loop_locks.clear()


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
        """Poll SET NX until acquired or `timeout` seconds elapse.

        Notes
        -----
        - A timeout of 0 makes exactly one attempt and returns, matching
          threading.Lock.acquire(timeout=0). Only None falls back to the
          configured lock_timeout - a falsy-check here would have turned an
          explicit 0 into a full-length wait.
        - Each poll iteration pays a full socket budget, so against an
          unreachable endpoint the wall time is driven by the socket
          timeouts, not by the 50 ms sleep.
        """
        if timeout is None:
            timeout = self._lock_timeout
        end = time.monotonic() + timeout
        while True:
            if self._client.set(
                self._key,
                self._token,
                nx=True,
                ex=max(1, round(self._lock_timeout)),
            ):
                self._acquired = True
                return True
            if not time.monotonic() < end:
                return False
            time.sleep(0.05)

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
        """Poll SET NX until acquired or `timeout` seconds elapse.

        Notes
        -----
        - A timeout of 0 makes exactly one attempt and returns; only None
          falls back to the configured lock_timeout.
        """
        if timeout is None:
            timeout = self._lock_timeout
        end = time.monotonic() + timeout
        while True:
            if await self._client.set(
                self._key,
                self._token,
                nx=True,
                ex=max(1, round(self._lock_timeout)),
            ):
                self._acquired = True
                return True
            if not time.monotonic() < end:
                return False
            await asyncio.sleep(0.05)

    async def release(self) -> None:
        if self._acquired:
            await self._client.eval(self._RELEASE_SCRIPT, 1, self._key, self._token)
            self._acquired = False


_DDB_POLL_BASE_DELAY = 0.05
_DDB_POLL_MAX_DELAY = 1.0


class DynamoDBMutex(CacheMutex):
    """Distributed lock using a DynamoDB conditional put.

    Parameters
    ----------
    client : Any
        Low-level boto3 'dynamodb' client.
    table_name : str
        Table holding the lock items.
    key : str
        Full lock item key, e.g. 'lock:<mangled cache key>'.
    lock_timeout : float, default 10.0
        Seconds after which an unreleased lock may be taken over, floored
        at 1 second exactly as the Redis mutex floors its EX.

    Notes
    -----
    - The lock item's schema matches `DynamoDBBackend`'s rows: the
      partition key is the SHA-256 digest of `key` (DynamoDB caps key
      values at 2048 bytes and lock keys embed cache keys), the readable
      key travels in 'key_text', and the integer 'expires_ttl' attribute
      lets one native-TTL specification garbage-collect abandoned locks
      along with expired entries.
    - DynamoDB has no server-side expiry at read time (native TTL deletes
      lazily), so takeover is part of the acquire condition itself:
      `attribute_not_exists(key) OR expires_at < now`. Conditional writes
      are evaluated against the most recently updated item version and
      writes apply in order, so two concurrent acquirers cannot both
      succeed.
    - `:now` is the acquirer's wall clock and 'expires_at' was written
      from the holder's, so takeover is exact only up to clock skew
      between hosts: this is a dogpile suppressor, not a correctness
      lock. Global tables reconcile last-writer-wins across Regions,
      which breaks the exclusion entirely - single-Region tables only.
    - Without the 1-second lifetime floor, a sub-second `lock_timeout`
      let every waiter's poll window reach the holder's expiry, so
      `on_lock_timeout='raise'` could never fire and lowering
      `lock_timeout` produced the very stampede it exists to shed.
    - A failed conditional put is still a billed write (minimum 1 WCU)
      against the one item everyone is waiting on, so the poll backs off
      exponentially instead of hammering a fixed 50 ms.
    """

    def __init__(
        self,
        client: Any,
        table_name: str,
        key: str,
        lock_timeout: float = 10.0,
    ) -> None:
        self._client = client
        self._table_name = table_name
        self._key = key
        self._hashed_key = hashlib.sha256(key.encode()).hexdigest()
        self._lock_timeout = lock_timeout
        self._token = str(uuid.uuid4())
        self._acquired = False

    def _try_put(self) -> bool:
        """Make exactly one conditional-put attempt to take the lock.
        """
        now = time.time()
        expires = now + max(self._lock_timeout, 1.0)
        try:
            self._client.put_item(
                TableName=self._table_name,
                Item={
                    'key': {'S': self._hashed_key},
                    'key_text': {'S': self._key},
                    'token': {'S': self._token},
                    'expires_at': {'N': str(expires)},
                    'expires_ttl': {'N': str(math.ceil(expires))},
                },
                ConditionExpression='attribute_not_exists(#k) OR #e < :now',
                ExpressionAttributeNames={'#k': 'key', '#e': 'expires_at'},
                ExpressionAttributeValues={':now': {'N': str(now)}},
            )
            return True
        except self._client.exceptions.ConditionalCheckFailedException:
            return False

    def acquire(self, timeout: float | None = None) -> bool:
        """Poll the conditional put until acquired or `timeout` seconds elapse.

        Notes
        -----
        - A timeout of 0 makes exactly one attempt and returns, matching
          threading.Lock.acquire(timeout=0). Only None falls back to the
          configured lock_timeout - a falsy-check here would have turned an
          explicit 0 into a full-length wait.
        - The poll delay doubles from 50 ms to a 1 s cap: every failed
          attempt is a billed write serialized on the contended item.
        - Each poll iteration also pays a full boto3 request budget, so
          against an unreachable endpoint the wall time is driven by the
          client's connect/read timeouts, not by the sleeps.
        """
        if timeout is None:
            timeout = self._lock_timeout
        end = time.monotonic() + timeout
        delay = _DDB_POLL_BASE_DELAY
        while True:
            if self._try_put():
                self._acquired = True
                return True
            if not time.monotonic() < end:
                return False
            time.sleep(delay)
            delay = min(delay * 2, _DDB_POLL_MAX_DELAY)

    def release(self) -> None:
        """Delete the lock item, but only while this mutex still owns it.

        Notes
        -----
        - The delete is conditional on the stored token: a lock that
          expired and was taken over by another caller must not be freed
          by the original holder's release. The failed condition is
          swallowed - by then the lock is simply no longer ours.
        """
        if self._acquired:
            try:
                self._client.delete_item(
                    TableName=self._table_name,
                    Key={'key': {'S': self._hashed_key}},
                    ConditionExpression='#t = :token',
                    ExpressionAttributeNames={'#t': 'token'},
                    ExpressionAttributeValues={':token': {'S': self._token}},
                )
            except self._client.exceptions.ConditionalCheckFailedException:
                pass
            self._acquired = False


class AsyncDynamoDBMutex(AsyncCacheMutex):
    """Async distributed lock delegating to `DynamoDBMutex` via threads.

    Notes
    -----
    - boto3 has no async client, so each single-attempt conditional put
      runs in a worker thread while the 50 ms poll wait stays on the event
      loop; only the request itself ever occupies a thread.
    """

    def __init__(
        self,
        client: Any,
        table_name: str,
        key: str,
        lock_timeout: float = 10.0,
    ) -> None:
        self._sync_mutex = DynamoDBMutex(client, table_name, key, lock_timeout)
        self._lock_timeout = lock_timeout

    async def acquire(self, timeout: float | None = None) -> bool:
        """Poll the conditional put until acquired or `timeout` seconds elapse.

        Notes
        -----
        - A timeout of 0 makes exactly one attempt and returns; only None
          falls back to the configured lock_timeout.
        - The poll delay doubles from 50 ms to a 1 s cap, exactly as the
          sync mutex backs off: every failed attempt is a billed write.
        """
        if timeout is None:
            timeout = self._lock_timeout
        end = time.monotonic() + timeout
        delay = _DDB_POLL_BASE_DELAY
        while True:
            if await asyncio.to_thread(self._sync_mutex.acquire, 0):
                return True
            if not time.monotonic() < end:
                return False
            await asyncio.sleep(delay)
            delay = min(delay * 2, _DDB_POLL_MAX_DELAY)

    async def release(self) -> None:
        await asyncio.to_thread(self._sync_mutex.release)


__all__ = [
    'CacheMutex',
    'AsyncCacheMutex',
    'NullMutex',
    'NullAsyncMutex',
    'ThreadingMutex',
    'AsyncioMutex',
    'RedisMutex',
    'AsyncRedisMutex',
    'DynamoDBMutex',
    'AsyncDynamoDBMutex',
]
