"""Tests for Redis backend internals: batched clear and SWR currsize.

These use lightweight fake clients so no real Redis is required.
"""
import fnmatch

import asyncio

from cachu.backends.redis import RedisBackend


class _RecordingSyncRedis:
    """Sync Redis stand-in that records delete/unlink batch sizes.
    """

    def __init__(self) -> None:
        self.store = {}
        self.unlink_calls = []
        self.delete_calls = []

    def scan_iter(self, match=None, count=None):
        for key in list(self.store):
            if match is None or fnmatch.fnmatch(key, match):
                yield key

    def unlink(self, *keys):
        self.unlink_calls.append(tuple(keys))
        for key in keys:
            self.store.pop(key, None)
        return len(keys)

    def delete(self, *keys):
        self.delete_calls.append(tuple(keys))
        for key in keys:
            self.store.pop(key, None)
        return len(keys)


class _RecordingAsyncRedis:
    """Async Redis stand-in that records delete/unlink batch sizes.
    """

    def __init__(self) -> None:
        self.store = {}
        self.unlink_calls = []
        self.delete_calls = []

    async def scan_iter(self, match=None, count=None):
        for key in list(self.store):
            if match is None or fnmatch.fnmatch(key, match):
                yield key

    async def unlink(self, *keys):
        self.unlink_calls.append(tuple(keys))
        for key in keys:
            self.store.pop(key, None)
        return len(keys)

    async def delete(self, *keys):
        self.delete_calls.append(tuple(keys))
        for key in keys:
            self.store.pop(key, None)
        return len(keys)


def test_clear_batches_deletes_into_few_round_trips():
    """Sync clear() batches deletes rather than one round trip per key.
    """
    backend = RedisBackend('redis://localhost:6379/0')
    fake = _RecordingSyncRedis()
    backend._sync_client = fake
    for i in range(250):
        fake.store[f'1m:test:fn|x={i}'] = b'v'

    cleared = backend.clear('1m:test:fn|*')

    assert cleared == 250
    assert fake.store == {}
    round_trips = len(fake.unlink_calls) + len(fake.delete_calls)
    assert round_trips <= 3, f'expected batched deletes, got {round_trips} round trips'


async def test_aclear_batches_deletes_into_few_round_trips():
    """Async aclear() batches deletes rather than one round trip per key.
    """
    backend = RedisBackend('redis://localhost:6379/0')
    fake = _RecordingAsyncRedis()
    backend._async_client = fake
    for i in range(250):
        fake.store[f'1m:test:fn|x={i}'] = b'v'

    cleared = await backend.aclear('1m:test:fn|*')

    assert cleared == 250
    assert fake.store == {}
    round_trips = len(fake.unlink_calls) + len(fake.delete_calls)
    assert round_trips <= 3, f'expected batched deletes, got {round_trips} round trips'


class _CurrsizeFakeClient:
    """Async client for currsize SWR tests.

    Implements mget/set/delete but intentionally omits get() so the SWR code
    must fetch fresh+last with a single MGET.
    """

    def __init__(self) -> None:
        self.data = {}

    async def mget(self, *keys):
        if len(keys) == 1 and isinstance(keys[0], (list, tuple)):
            keys = tuple(keys[0])
        return [self.data.get(key) for key in keys]

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return None
        self.data[key] = value
        return True

    async def delete(self, key):
        self.data.pop(key, None)


class _CurrsizeFakeBackend:
    """Minimal RedisBackend stand-in for currsize SWR tests.
    """

    def __init__(self) -> None:
        self._client = _CurrsizeFakeClient()

    def _get_async_client(self):
        return self._client

    async def acount(self, pattern):
        return 3


async def test_currsize_cold_start_uses_mget_and_returns_zero():
    """Cold start returns 0 and reads fresh+last via a single MGET (no per-key GET).
    """
    from cachu import decorator

    decorator._background_tasks.clear()
    backend = _CurrsizeFakeBackend()

    result = await decorator._get_cached_currsize_async(
        backend, 'pkg', 'fn', '*:test:fn|*')

    assert result == 0
    await asyncio.gather(*list(decorator._background_tasks))


async def test_currsize_refresh_task_is_strongly_referenced():
    """The background refresh task is retained so it cannot be GC'd mid-flight.
    """
    from cachu import decorator

    decorator._background_tasks.clear()
    backend = _CurrsizeFakeBackend()

    await decorator._get_cached_currsize_async(backend, 'pkg', 'fn', '*:test:fn|*')

    assert len(decorator._background_tasks) >= 1
    await asyncio.gather(*list(decorator._background_tasks))
