"""Redis cache backend implementation.
"""
import asyncio
import functools
import logging
import pickle
import struct
import threading
import time
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any, Literal

from ..api import NO_VALUE, Backend
from ..mutex import AsyncCacheMutex, AsyncRedisMutex, CacheMutex, RedisMutex

if TYPE_CHECKING:
    import redis
    import redis.asyncio as aioredis


logger = logging.getLogger(__name__)

_METADATA_FORMAT = 'd'
_METADATA_SIZE = struct.calcsize(_METADATA_FORMAT)
_STATS_KEY_PREFIX = 'cachu:stats:'
_CLEAR_BATCH_SIZE = 500
_MIN_CONNECT_SLICE = 0.001
_MIN_CONNECT_FRACTION = 0.2

_CURRSIZE_FRESH_PREFIX = 'cachu:_currsize:'
_CURRSIZE_LAST_PREFIX = 'cachu:_currsize_last:'
_CURRSIZE_LOCK_PREFIX = 'cachu:_currsize_lock:'

# Notes:
# - The cached counts a clear must invalidate, deliberately excluding the
#   refresh lock: deleting a lock another caller holds is the same fault as
#   clearing a live dogpile mutex.
_CURRSIZE_CACHE_PREFIXES = (_CURRSIZE_FRESH_PREFIX, _CURRSIZE_LAST_PREFIX)


def _get_redis_module() -> Any:
    """Import redis module, raising helpful error if not installed.
    """
    try:
        import redis
        return redis
    except ImportError as e:
        raise RuntimeError(
            "Redis support requires the 'redis' package. Install with: pip install cachu[redis]"
        ) from e


def _get_async_redis_module() -> Any:
    """Import redis.asyncio module, raising helpful error if not installed.
    """
    try:
        import redis.asyncio as aioredis
        return aioredis
    except ImportError as e:
        raise RuntimeError(
            "Async Redis support requires the 'redis' package (>=4.2.0). "
            "Install with: pip install cachu[redis]"
        ) from e


class _ConnectBudgetMixin:
    """Share one `socket_connect_timeout` across a whole sync connect attempt.

    Notes
    -----
    - redis-py's sync `Connection._connect` loops over every address
      `socket.getaddrinfo` returns and applies `socket_connect_timeout` to
      EACH of them. An endpoint that resolves to n A records - an
      ElastiCache serverless endpoint does - therefore costs n budgets per
      attempt, so a blackholed endpoint blocks for
      `redis_socket_timeout * n * (1 + redis_retry_count)` where cachu
      documents and warns about `redis_socket_timeout * (1 + retry_count)`.
      That is a hang rather than an exception, so neither `fail_open` nor a
      caller's `try`/`except` shortens it.
    - The override reports the timeout as "budget left for THIS attempt",
      which is the value redis-py already re-reads once per address, so the
      address loop shrinks to fit the budget instead of restarting the clock
      at every address.
    - Each address is nevertheless guaranteed `_MIN_CONNECT_FRACTION` of the
      budget, which is what keeps redis-py's per-address FAILOVER alive. A
      flat floor of a millisecond does not: measured against a two-address
      endpoint whose first address blackholes, a healthy second address
      answering in 1.5 ms or slower became unreachable, so one bad address
      in an endpoint turned a slow cache into an unavailable one and
      `fail_open` sent every call to the origin. Any cross-AZ hop is above
      that threshold. Losing availability to gain a latency bound is the
      wrong trade.
    - The cost of that guarantee is that the ceiling is not flat: one
      attempt costs at most `budget * (1 + (n - 1) * _MIN_CONNECT_FRACTION)`
      - 1.4x at n=3 rather than the 3x redis-py would spend. A flat ceiling
      is unreachable together with failover, since an address that hangs for
      the whole budget leaves nothing for the next one. n=1 is unaffected:
      the first address always gets the full budget.
    - `_MIN_CONNECT_SLICE` is a second, absolute floor. `settimeout(0)` puts
      the socket in NON-BLOCKING mode and a negative value raises
      ValueError, so a tiny configured budget must still yield something
      positive.
    - Only the sync client needs this. redis-py's async `_connect` already
      wraps its whole address loop in a single
      `async_timeout(socket_connect_timeout)`.
    - Bounds the CONNECT, and DNS runs inside it because `getaddrinfo` is
      that loop's iterable - a slow resolver eats the budget the addresses
      would have had, down to the per-address guarantee. No redis-py timeout
      bounds resolution either way, so a wedged resolver stays a host-level
      concern (`resolv.conf` `timeout`/`attempts`).
    """

    _socket_connect_timeout: float | None = None
    _connect_deadline: float | None = None

    @property
    def socket_connect_timeout(self) -> float | None:
        """Seconds this address may spend: the budget left, floored at a share of it.
        """
        budget = self._socket_connect_timeout
        if budget is None or self._connect_deadline is None:
            return budget
        floor = max(_MIN_CONNECT_SLICE, budget * _MIN_CONNECT_FRACTION)
        remaining = self._connect_deadline - time.monotonic()
        return max(floor, min(budget, remaining))

    @socket_connect_timeout.setter
    def socket_connect_timeout(self, value: float | None) -> None:
        """Store the configured budget, mirroring redis-py's own setter.
        """
        self._socket_connect_timeout = value

    def _connect(self) -> Any:
        """Open the budget, delegate to redis-py, and always close it again.
        """
        budget = self._socket_connect_timeout
        if not budget:
            return super()._connect()

        self._connect_deadline = time.monotonic() + budget
        try:
            return super()._connect()
        finally:
            self._connect_deadline = None


@functools.lru_cache(maxsize=None)
def _connect_budget_class(base: type) -> type:
    """Build the `_ConnectBudgetMixin` subclass of a redis-py connection class.

    Parameters
    ----------
    base : type
        Connection class redis-py resolved for the URL - `Connection`,
        `SSLConnection` or `UnixDomainSocketConnection`.

    Returns
    -------
    type
        Subclass of `base` whose connect attempt is bounded as a whole.

    Notes
    -----
    - Derived from whatever the pool resolved rather than chosen from the
      URL scheme, so TLS and unix-socket URLs keep their own connection
      behaviour and only the timeout accounting changes.
    - Cached, so one class exists per base and `isinstance` checks and
      connection construction stay cheap.
    """
    return type(f'ConnectBudget{base.__name__}', (_ConnectBudgetMixin, base), {})


def get_redis_client(
    url: str,
    health_check_interval: int = 30,
    socket_timeout: float = 5.0,
    retry_count: int = 3,
) -> 'redis.Redis':
    """Create a Redis client from URL with connection resilience.

    Parameters
    ----------
    url : str
        Redis URL, `redis://`, `rediss://` or `unix://`.
    health_check_interval : int, default 30
        Seconds between redis-py connection health checks.
    socket_timeout : float, default 5.0
        Passed as both `socket_timeout` and `socket_connect_timeout`, and
        shared across one whole connect attempt rather than applied per
        resolved address.
    retry_count : int, default 3
        Retry attempts redis-py makes per operation.

    Returns
    -------
    redis.Redis
        Client whose pool builds connect-budgeted connections.

    Notes
    -----
    - One connect attempt costs at most
      `socket_timeout * (1 + (n - 1) * _MIN_CONNECT_FRACTION)` for an
      endpoint resolving to n addresses - 1.4x the budget at n=3 - against
      the `socket_timeout * n` redis-py would spend. The residual term is
      the per-address guarantee that keeps failover working; see
      `_ConnectBudgetMixin`.
    """
    redis_module = _get_redis_module()
    retry = redis_module.retry.Retry(
        redis_module.backoff.ExponentialBackoff(), retries=retry_count)
    client = redis_module.from_url(
        url,
        health_check_interval=health_check_interval,
        socket_connect_timeout=socket_timeout,
        socket_timeout=socket_timeout,
        retry_on_timeout=True,
        retry=retry,
    )
    pool = client.connection_pool
    pool.connection_class = _connect_budget_class(pool.connection_class)
    return client


def get_async_redis_client(
    url: str,
    health_check_interval: int = 30,
    socket_timeout: float = 5.0,
    retry_count: int = 3,
) -> 'aioredis.Redis':
    """Create an async Redis client from URL with connection resilience.
    """
    aioredis = _get_async_redis_module()
    redis_module = _get_redis_module()
    retry = redis_module.retry.Retry(
        redis_module.backoff.ExponentialBackoff(), retries=retry_count)
    return aioredis.from_url(
        url,
        health_check_interval=health_check_interval,
        socket_connect_timeout=socket_timeout,
        socket_timeout=socket_timeout,
        retry_on_timeout=True,
        retry=retry,
    )


def _pack_value(value: Any, created_at: float) -> bytes:
    """Pack value with creation timestamp.
    """
    metadata = struct.pack(_METADATA_FORMAT, created_at)
    pickled = pickle.dumps(value)
    return metadata + pickled


def _unpack_value(data: bytes, key: str) -> tuple[Any, float] | None:
    """Unpack value and creation timestamp.

    Parameters
    ----------
    data : bytes
        Packed metadata header followed by the pickled value.
    key : str
        Cache key, used only to identify the row in the eviction warning.

    Returns
    -------
    tuple of (Any, float) or None
        The value and its creation timestamp, or None when the payload
        cannot be decoded.

    Notes
    -----
    - An undecodable payload is treated as a miss rather than an error, per
      dogpile.cache, so the caller degrades gracefully.
    - It is logged because the usual cause is a deploy that changed a
      pickled class while an older release still writes the same key, which
      drives the hit rate to zero for as long as both run. Silent here would
      leave the operator nothing to read; the file backend already warns on
      the identical fault.
    """
    try:
        created_at = struct.unpack(_METADATA_FORMAT, data[:_METADATA_SIZE])[0]
        value = pickle.loads(data[_METADATA_SIZE:])
        return value, created_at
    except (pickle.UnpicklingError, EOFError, TypeError, AttributeError, ModuleNotFoundError, struct.error):
        logger.warning(
            f'Evicting undecodable cache row for key {key!r}', exc_info=True)
        return None


class RedisBackend(Backend):
    """Unified Redis cache backend with both sync and async interfaces.
    """

    def __init__(
        self,
        url: str,
        lock_timeout: float = 10.0,
        health_check_interval: int = 30,
        socket_timeout: float = 5.0,
        retry_count: int = 3,
    ) -> None:
        self._url = url
        self._lock_timeout = lock_timeout
        self._health_check_interval = health_check_interval
        self._socket_timeout = socket_timeout
        self._retry_count = retry_count
        self._sync_client: redis.Redis | None = None
        self._async_client: aioredis.Redis | None = None
        self._init_lock = threading.Lock()

    @property
    def client(self) -> 'redis.Redis':
        """Lazy-load sync Redis client.
        """
        if self._sync_client is None:
            self._sync_client = get_redis_client(
                self._url,
                self._health_check_interval,
                self._socket_timeout,
                self._retry_count,
            )
        return self._sync_client

    def _get_async_client(self) -> 'aioredis.Redis':
        """Lazy-load async Redis client (from_url is NOT async).
        """
        with self._init_lock:
            if self._async_client is None:
                self._async_client = get_async_redis_client(
                    self._url,
                    self._health_check_interval,
                    self._socket_timeout,
                    self._retry_count,
                )
            return self._async_client

    # ===== Sync interface =====

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found or corrupted.
        """
        data = self.client.get(key)
        if data is None:
            return NO_VALUE
        result = _unpack_value(data, key)
        if result is None:
            self.client.delete(key)
            return NO_VALUE
        return result[0]

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found or corrupted.
        """
        data = self.client.get(key)
        if data is None:
            return NO_VALUE, None
        result = _unpack_value(data, key)
        if result is None:
            self.client.delete(key)
            return NO_VALUE, None
        return result

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds. A non-positive TTL is not cached.

        Notes
        -----
        - Uses SET ... EX rather than SETEX: redis-py 8.x deprecates setex in
          favour of set(ex=...), and the wire semantics are identical.
        """
        if ttl <= 0:
            self.client.delete(key)
            return
        now = time.time()
        packed = _pack_value(value, now)
        self.client.set(key, packed, ex=ttl)

    def delete(self, key: str) -> None:
        """Delete value by key.
        """
        self.client.delete(key)

    def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.

        Parameters
        ----------
        pattern : str or None, default None
            Redis glob to match. None means the ENTIRE logical DB, cachu-owned
            or not.

        Returns
        -------
        int
            Number of keys UNLINKed.

        Notes
        -----
        - Single-key UNLINKs are pipelined to stay legal on Redis Cluster.
        - `cache_clear` never passes None: it derives a region-scoped glob
          from cachu's own key shape, so a library-level clear cannot reach a
          key cachu did not write. None stays available on the backend itself
          because a caller reaching for a backend object directly is asking
          for exactly that store.
        """
        if pattern is None:
            pattern = '*'

        keys = list(self.client.scan_iter(match=pattern))
        count = 0
        for start in range(0, len(keys), _CLEAR_BATCH_SIZE):
            chunk = keys[start:start + _CLEAR_BATCH_SIZE]
            pipe = self.client.pipeline(transaction=False)
            for key in chunk:
                pipe.unlink(key)
            pipe.execute()
            count += len(chunk)
        return count

    def keys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys matching pattern.
        """
        redis_pattern = pattern or '*'
        for key in self.client.scan_iter(match=redis_pattern):
            yield key.decode() if isinstance(key, bytes) else key

    def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.

        Whole-keyspace counts (pattern is None or '*') use DBSIZE for O(1).
        A specific pattern still requires a SCAN.
        """
        if pattern is None or pattern == '*':
            return self.client.dbsize()
        return sum(1 for _ in self.keys(pattern))

    def get_mutex(self, key: str) -> CacheMutex:
        """Get a mutex for dogpile prevention on the given key.
        """
        return RedisMutex(self.client, f'lock:{key}', self._lock_timeout)

    # ===== Stats interface (sync) =====

    def incr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Increment a stat counter for a function.
        """
        self.client.hincrby(f'{_STATS_KEY_PREFIX}{fn_name}', stat, 1)

    def get_stats(self, fn_name: str) -> tuple[int, int]:
        """Get (hits, misses) for a function.
        """
        data = self.client.hgetall(f'{_STATS_KEY_PREFIX}{fn_name}')
        return (int(data.get(b'hits', 0)), int(data.get(b'misses', 0)))

    def clear_stats(self, fn_name: str | None = None) -> None:
        """Clear stats for a function, or all stats if fn_name is None.

        Notes
        -----
        - Also drops the cached `currsize` counts, which are stats too: they
          are served from a stale-while-revalidate cache, so a clear that
          left them standing would make `cache_info` report the pre-clear
          size for up to `_CURRSIZE_FRESH_TTL` seconds.
        - The refresh LOCK is deliberately left alone. Deleting a lock
          another caller holds is the same fault as clearing a live dogpile
          mutex; it self-heals in at most `_CURRSIZE_LOCK_TTL` and only
          costs one extra scan.
        """
        if fn_name:
            self.client.delete(f'{_STATS_KEY_PREFIX}{fn_name}')
        else:
            for key in self.client.scan_iter(match=f'{_STATS_KEY_PREFIX}*'):
                self.client.delete(key)
        for prefix in _CURRSIZE_CACHE_PREFIXES:
            for key in self.client.scan_iter(match=f'{prefix}*'):
                self.client.delete(key)

    # ===== Async interface =====

    async def aget(self, key: str) -> Any:
        """Async get value by key. Returns NO_VALUE if not found or corrupted.
        """
        client = self._get_async_client()
        data = await client.get(key)
        if data is None:
            return NO_VALUE
        result = _unpack_value(data, key)
        if result is None:
            await client.delete(key)
            return NO_VALUE
        return result[0]

    async def aget_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Async get value and creation timestamp. Returns (NO_VALUE, None) if not found or corrupted.
        """
        client = self._get_async_client()
        data = await client.get(key)
        if data is None:
            return NO_VALUE, None
        result = _unpack_value(data, key)
        if result is None:
            await client.delete(key)
            return NO_VALUE, None
        return result

    async def aset(self, key: str, value: Any, ttl: int) -> None:
        """Async set value with TTL in seconds. A non-positive TTL is not cached.

        Notes
        -----
        - Uses SET ... EX rather than SETEX: redis-py 8.x deprecates setex in
          favour of set(ex=...), and the wire semantics are identical.
        """
        client = self._get_async_client()
        if ttl <= 0:
            await client.delete(key)
            return
        now = time.time()
        packed = _pack_value(value, now)
        await client.set(key, packed, ex=ttl)

    async def adelete(self, key: str) -> None:
        """Async delete value by key.
        """
        client = self._get_async_client()
        await client.delete(key)

    async def aclear(self, pattern: str | None = None) -> int:
        """Async clear entries matching pattern. Returns count of cleared entries.

        Parameters
        ----------
        pattern : str or None, default None
            Redis glob to match. None means the ENTIRE logical DB, exactly as
            in the sync `clear`; `async_cache_clear` never passes it.

        Returns
        -------
        int
            Number of keys UNLINKed.

        Notes
        -----
        - Single-key UNLINKs are pipelined to stay legal on Redis Cluster.
        """
        client = self._get_async_client()
        if pattern is None:
            pattern = '*'

        keys = [key async for key in client.scan_iter(match=pattern)]
        count = 0
        for start in range(0, len(keys), _CLEAR_BATCH_SIZE):
            chunk = keys[start:start + _CLEAR_BATCH_SIZE]
            pipe = client.pipeline(transaction=False)
            for key in chunk:
                pipe.unlink(key)
            await pipe.execute()
            count += len(chunk)
        return count

    async def akeys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Async iterate over keys matching pattern.
        """
        client = self._get_async_client()
        redis_pattern = pattern or '*'
        async for key in client.scan_iter(match=redis_pattern):
            yield key.decode() if isinstance(key, bytes) else key

    async def acount(self, pattern: str | None = None) -> int:
        """Async count keys matching pattern.

        Whole-keyspace counts (pattern is None or '*') use DBSIZE for O(1).
        A specific pattern still requires a SCAN.
        """
        if pattern is None or pattern == '*':
            return await self._get_async_client().dbsize()
        count = 0
        async for _ in self.akeys(pattern):
            count += 1
        return count

    def get_async_mutex(self, key: str) -> AsyncCacheMutex:
        """Get an async mutex for dogpile prevention on the given key.
        """
        return AsyncRedisMutex(self._get_async_client(), f'lock:{key}', self._lock_timeout)

    # ===== Stats interface (async) =====

    async def aincr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Async increment a stat counter for a function.
        """
        client = self._get_async_client()
        await client.hincrby(f'{_STATS_KEY_PREFIX}{fn_name}', stat, 1)

    async def aget_stats(self, fn_name: str) -> tuple[int, int]:
        """Async get (hits, misses) for a function.
        """
        client = self._get_async_client()
        data = await client.hgetall(f'{_STATS_KEY_PREFIX}{fn_name}')
        return (int(data.get(b'hits', 0)), int(data.get(b'misses', 0)))

    async def aclear_stats(self, fn_name: str | None = None) -> None:
        """Async clear stats for a function, or all stats if fn_name is None.

        Notes
        -----
        - Drops the cached `currsize` counts alongside the counters, and
          leaves the refresh lock alone, exactly as the sync `clear_stats`
          does.
        """
        client = self._get_async_client()
        if fn_name:
            await client.delete(f'{_STATS_KEY_PREFIX}{fn_name}')
        else:
            async for key in client.scan_iter(match=f'{_STATS_KEY_PREFIX}*'):
                await client.delete(key)
        for prefix in _CURRSIZE_CACHE_PREFIXES:
            async for key in client.scan_iter(match=f'{prefix}*'):
                await client.delete(key)

    # ===== Lifecycle =====

    def _close_sync_client(self) -> None:
        """Close sync client if open.
        """
        if self._sync_client is not None:
            client = self._sync_client
            self._sync_client = None
            client.close()

    def _close_async_client_sync(self) -> None:
        """Close async client from sync context via thread.
        """
        if self._async_client is not None:
            client = self._async_client
            self._async_client = None

            async def _close() -> None:
                try:
                    await client.aclose()
                except RuntimeError:
                    pass

            t = threading.Thread(target=lambda: asyncio.run(_close()))
            t.start()
            t.join(timeout=5.0)

    def close(self) -> None:
        """Close all backend resources from sync context.
        """
        self._close_sync_client()
        self._close_async_client_sync()

    async def aclose(self) -> None:
        """Close all backend resources from async context.
        """
        if self._async_client is not None:
            client = self._async_client
            self._async_client = None
            await client.aclose()
        self._close_sync_client()
