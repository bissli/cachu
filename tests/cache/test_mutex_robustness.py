"""Tests for mutex correctness across event loops and lock-TTL edge cases.
"""
import asyncio

from cachu.mutex import AsyncioMutex, AsyncRedisMutex, RedisMutex


def test_asyncio_mutex_independent_lock_per_event_loop():
    """Each event loop gets its own lock instance for the same key.

    A process-global asyncio.Lock cached by key binds to the first loop that
    awaits it and breaks (or errors) on a second loop. The registry must hand a
    distinct lock to each loop. Both loops are kept alive simultaneously so the
    two lock objects can be compared by identity.
    """
    AsyncioMutex.clear_locks()
    key = 'cross-loop-key'
    loop1 = asyncio.new_event_loop()
    loop2 = asyncio.new_event_loop()

    async def use():
        mutex = AsyncioMutex(key)
        assert await mutex.acquire(timeout=1) is True
        await mutex.release()
        return mutex._lock

    try:
        lock1 = loop1.run_until_complete(use())
        lock2 = loop2.run_until_complete(use())
    finally:
        loop1.close()
        loop2.close()

    assert lock1 is not lock2


def test_asyncio_mutex_excludes_within_one_loop():
    """Within a single loop the per-key lock still serializes holders.
    """
    async def scenario() -> None:
        first = AsyncioMutex('same')
        second = AsyncioMutex('same')
        assert await first.acquire(timeout=1) is True
        assert await second.acquire(timeout=0.1) is False
        await first.release()
        assert await second.acquire(timeout=1) is True
        await second.release()

    asyncio.run(scenario())


class _RecordingSyncRedis:
    """Records the ex= value passed to SET so we can assert it is never 0.
    """

    def __init__(self) -> None:
        self.ex_values = []

    def set(self, key, value, nx=True, ex=None):
        self.ex_values.append(ex)
        return True

    def eval(self, *args):
        return 1


class _RecordingAsyncRedis:
    """Async variant of the recording SET stand-in.
    """

    def __init__(self) -> None:
        self.ex_values = []

    async def set(self, key, value, nx=True, ex=None):
        self.ex_values.append(ex)
        return True

    async def eval(self, *args):
        return 1


def test_redis_mutex_subsecond_timeout_not_floored_to_zero():
    """A sub-second lock_timeout must not produce EX 0 (rejected by Redis).
    """
    fake = _RecordingSyncRedis()
    mutex = RedisMutex(fake, 'lock:k', lock_timeout=0.5)

    assert mutex.acquire() is True
    assert fake.ex_values
    assert all(ex is None or ex >= 1 for ex in fake.ex_values)


async def test_async_redis_mutex_subsecond_timeout_not_floored_to_zero():
    """A sub-second lock_timeout must not produce EX 0 on the async mutex.
    """
    fake = _RecordingAsyncRedis()
    mutex = AsyncRedisMutex(fake, 'lock:k', lock_timeout=0.5)

    assert await mutex.acquire() is True
    assert fake.ex_values
    assert all(ex is None or ex >= 1 for ex in fake.ex_values)
