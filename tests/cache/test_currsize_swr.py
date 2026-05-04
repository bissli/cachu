"""Tests for stale-while-revalidate currsize and DBSIZE short-circuit.
"""
import asyncio

import pytest
import redis as redis_lib
from cachu.backends.redis import RedisBackend
from cachu.decorator import _CURRSIZE_FRESH_TTL
from cachu.decorator import _currsize_keys
from cachu.decorator import _get_cached_currsize_async

pytestmark = pytest.mark.redis


@pytest.fixture
def redis_url(redis_docker):
    """Build the Redis URL for direct access in tests.
    """
    from _fixtures.redis import redis_test_config

    return f'redis://{redis_test_config.host}:{redis_test_config.port}/0'


@pytest.fixture
def backend(redis_url):
    """Provide a fresh RedisBackend for each test.
    """
    return RedisBackend(redis_url)


@pytest.fixture
def sync_redis(redis_docker):
    """Provide a sync redis client for direct manipulation in tests.
    """
    from _fixtures.redis import redis_test_config

    client = redis_lib.Redis(host=redis_test_config.host, port=redis_test_config.port, db=0)
    yield client
    client.close()


@pytest.mark.asyncio
async def test_acount_none_uses_dbsize(backend, sync_redis):
    """acount(None) and acount('*') should return DBSIZE rather than scan-counting.
    """
    for i in range(7):
        sync_redis.set(f'unrelated:key:{i}', b'v')

    assert await backend.acount(None) == 7
    assert await backend.acount('*') == 7


@pytest.mark.asyncio
async def test_currsize_cold_start_returns_zero_and_schedules_refresh(backend, sync_redis):
    """Cold start: no fresh, no last-known. Returns 0 immediately and the
    background refresh populates both keys.
    """
    sync_redis.set('1m:test:fn|x=1', b'cached1')
    sync_redis.set('1m:test:fn|x=2', b'cached2')

    pattern = '*:test:fn|*'
    fresh_key, last_key, lock_key = _currsize_keys('pkg', 'fn')

    result = await _get_cached_currsize_async(backend, 'pkg', 'fn', pattern)
    assert result == 0

    for _ in range(50):
        if sync_redis.get(fresh_key) is not None:
            break
        await asyncio.sleep(0.05)

    assert int(sync_redis.get(fresh_key)) == 2
    assert int(sync_redis.get(last_key)) == 2
    assert sync_redis.get(lock_key) is None


@pytest.mark.asyncio
async def test_currsize_serves_fresh_value(backend, sync_redis):
    """A fresh cached value is returned without invoking the slow scan.
    """
    fresh_key, _, _ = _currsize_keys('pkg', 'fn')
    sync_redis.set(fresh_key, 42, ex=_CURRSIZE_FRESH_TTL)

    async def boom(*args, **kwargs):
        raise AssertionError('acount must not be called when fresh value is present')

    backend.acount = boom

    assert await _get_cached_currsize_async(backend, 'pkg', 'fn', '*:test:fn|*') == 42


@pytest.mark.asyncio
async def test_currsize_serves_stale_during_refresh(backend, sync_redis):
    """No fresh key but a last-known value: returns stale immediately and refreshes.
    """
    fresh_key, last_key, _ = _currsize_keys('pkg', 'fn')
    sync_redis.set(last_key, 99)
    for i in range(3):
        sync_redis.set(f'1m:test:fn|x={i}', b'cached')

    result = await _get_cached_currsize_async(backend, 'pkg', 'fn', '*:test:fn|*')
    assert result == 99

    for _ in range(50):
        if sync_redis.get(fresh_key) is not None:
            break
        await asyncio.sleep(0.05)

    assert int(sync_redis.get(fresh_key)) == 3
    assert int(sync_redis.get(last_key)) == 3


@pytest.mark.asyncio
async def test_currsize_concurrent_misses_run_one_scan(backend, sync_redis):
    """Concurrent cold-cache callers acquire the lock once; only one scan runs.
    """
    sync_redis.set('1m:test:fn|x=1', b'cached')

    fresh_key, _, _ = _currsize_keys('pkg', 'fn')
    scan_calls = 0
    original_acount = backend.acount

    async def counting_acount(pattern):
        nonlocal scan_calls
        scan_calls += 1
        await asyncio.sleep(0.1)
        return await original_acount(pattern)

    backend.acount = counting_acount

    results = await asyncio.gather(*[
        _get_cached_currsize_async(backend, 'pkg', 'fn', '*:test:fn|*')
        for _ in range(10)
        ])

    assert all(r == 0 for r in results)

    for _ in range(50):
        if sync_redis.get(fresh_key) is not None:
            break
        await asyncio.sleep(0.05)

    assert scan_calls == 1
