"""Tests that a non-positive TTL is treated uniformly as 'do not cache'.

Redis SETEX rejects ttl<=0 (raising), while memory/sqlite silently stored a
value that expired immediately; the contract should be identical everywhere.
"""
from cachu.api import NO_VALUE
from cachu.backends.memory import MemoryBackend
from cachu.backends.redis import RedisBackend
from cachu.backends.sqlite import SqliteBackend


def test_memory_zero_ttl_not_cached():
    """Memory backend: ttl=0 stores nothing retrievable.
    """
    backend = MemoryBackend()
    backend.set('k', 'v', 0)
    assert backend.get('k') is NO_VALUE


def test_memory_negative_ttl_not_cached():
    """Memory backend: negative ttl stores nothing retrievable.
    """
    backend = MemoryBackend()
    backend.set('k', 'v', -5)
    assert backend.get('k') is NO_VALUE


def test_sqlite_zero_ttl_not_cached(tmp_path):
    """SQLite backend: ttl=0 stores nothing retrievable.
    """
    backend = SqliteBackend(str(tmp_path / 'cache.db'))
    backend.set('k', 'v', 0)
    assert backend.get('k') is NO_VALUE


class _FakeSetexRedis:
    """Sync Redis stand-in that rejects non-positive expirations like real Redis.
    """

    def __init__(self) -> None:
        self.store = {}

    def setex(self, key, ttl, value):
        if ttl <= 0:
            raise ValueError(f'invalid expire time in setex: {ttl}')
        self.store[key] = value

    def get(self, key):
        return self.store.get(key)

    def delete(self, *keys):
        for key in keys:
            self.store.pop(key, None)


def test_redis_zero_ttl_does_not_raise_and_skips():
    """Redis backend: ttl<=0 must not raise (no SETEX 0) and must not cache.
    """
    backend = RedisBackend('redis://localhost:6379/0')
    backend._sync_client = _FakeSetexRedis()

    backend.set('k', 'v', 0)
    assert backend.get('k') is NO_VALUE
