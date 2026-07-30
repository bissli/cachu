"""Tests that Redis writes use SET ... EX rather than the deprecated SETEX.

Notes
-----
- redis-py 8.x warns that setex is deprecated in favour of set with an
  expiry argument, so every cache write emitted a DeprecationWarning.
- The wire semantics are identical, but the argument order is not (SETEX
  takes key, seconds, value; SET takes key, value, ex=seconds), so a silent
  swap would store the TTL as the value.
"""
import warnings

import cachu
import pytest
from cachu.backends.redis import RedisBackend, _unpack_value


class _WriteRecordingRedis:
    """Sync Redis stand-in that records writes and rejects setex.
    """

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.expiries: dict[str, int] = {}
        self.set_calls: list[tuple] = []

    def set(self, name, value, ex=None, **kwargs):
        self.set_calls.append((name, value, ex))
        self.store[name] = value
        if ex is not None:
            self.expiries[name] = ex
        return True

    def setex(self, name, time, value):
        raise AssertionError('setex is deprecated in redis-py 8.x; use set(ex=...)')

    def get(self, name):
        return self.store.get(name)

    def delete(self, *names):
        for name in names:
            self.store.pop(name, None)
            self.expiries.pop(name, None)
        return len(names)


class _AsyncWriteRecordingRedis(_WriteRecordingRedis):
    """Async Redis stand-in with the same recording behaviour.
    """

    async def set(self, name, value, ex=None, **kwargs):
        return _WriteRecordingRedis.set(self, name, value, ex=ex, **kwargs)

    async def setex(self, name, time, value):
        raise AssertionError('setex is deprecated in redis-py 8.x; use set(ex=...)')

    async def get(self, name):
        return self.store.get(name)

    async def delete(self, *names):
        return _WriteRecordingRedis.delete(self, *names)


@pytest.fixture
def sync_backend():
    """RedisBackend wired to a recording sync client.
    """
    backend = RedisBackend('redis://localhost:6379/0')
    client = _WriteRecordingRedis()
    backend._sync_client = client
    return backend, client


@pytest.fixture
def async_backend():
    """RedisBackend wired to a recording async client.
    """
    backend = RedisBackend('redis://localhost:6379/0')
    client = _AsyncWriteRecordingRedis()
    backend._async_client = client
    return backend, client


class TestSyncWrite:
    """The sync write path uses SET with an expiry.
    """

    def test_set_is_used_with_the_ttl_as_expiry(self, sync_backend):
        """A write issues SET key value EX ttl, not SETEX.

        Mutation: restore client.setex(key, ttl, packed), which the stand-in
        rejects outright.
        Oracle: the TTL as written, 300, in the ex keyword.
        """
        backend, client = sync_backend

        backend.set('k', {'v': 1}, 300)

        assert len(client.set_calls) == 1
        name, _, ex = client.set_calls[0]
        assert name == 'k'
        assert ex == 300

    def test_value_and_ttl_are_not_transposed(self, sync_backend):
        """The payload lands in the value slot, not the expiry slot.

        Mutation: call set(key, ttl, ex=packed) - the classic SETEX-to-SET
        argument-order slip, which would store the integer TTL as the value.
        Oracle: the original object, round-tripped back through get().
        """
        backend, client = sync_backend

        backend.set('k', {'v': 1}, 300)

        assert backend.get('k') == {'v': 1}
        assert _unpack_value(client.store['k'], 'k')[0] == {'v': 1}

    def test_non_positive_ttl_deletes_instead_of_writing(self, sync_backend):
        """A ttl of 0 still deletes rather than writing an eternal key.

        Mutation: drop the ttl <= 0 guard, so set(key, value, ex=0) raises or
        writes a key that never expires.
        Oracle: absence of the key and of any SET call.
        """
        backend, client = sync_backend

        backend.set('k', 'value', 0)

        assert client.set_calls == []
        assert 'k' not in client.store


class TestAsyncWrite:
    """The async write path uses SET with an expiry.
    """

    async def test_aset_uses_set_with_the_ttl_as_expiry(self, async_backend):
        """An async write issues SET key value EX ttl, not SETEX.

        Mutation: restore client.setex on the async path only.
        Oracle: the TTL as written, 300, in the ex keyword.
        """
        backend, client = async_backend

        await backend.aset('k', {'v': 1}, 300)

        assert len(client.set_calls) == 1
        assert client.set_calls[0][2] == 300

    async def test_async_value_and_ttl_are_not_transposed(self, async_backend):
        """The async payload lands in the value slot.

        Mutation: transpose value and expiry on the async path.
        Oracle: the original object, round-tripped back through aget().
        """
        backend, _ = async_backend

        await backend.aset('k', {'v': 1}, 300)

        assert await backend.aget('k') == {'v': 1}

    async def test_async_non_positive_ttl_deletes(self, async_backend):
        """An async ttl of 0 deletes rather than writing.

        Mutation: drop the ttl <= 0 guard on the async path.
        Oracle: absence of the key and of any SET call.
        """
        backend, client = async_backend

        await backend.aset('k', 'value', 0)

        assert client.set_calls == []
        assert 'k' not in client.store


@pytest.mark.redis
class TestAgainstRealRedis:
    """The expiry really reaches the server.
    """

    def test_ttl_is_applied_on_the_server(self, redis_docker):
        """A real Redis reports a TTL close to the one requested.

        Mutation: pass the TTL positionally into the value slot, which a
        fake client would happily accept but a server would not honour.
        Oracle: the server's own TTL command, bounded by the requested 300s.
        """
        backend = cachu.get_backend('redis', ttl=300)
        backend.set('ttl-probe', 'value', 300)

        remaining = backend.client.ttl('ttl-probe')

        assert 0 < remaining <= 300
        assert backend.get('ttl-probe') == 'value'

    def test_write_emits_no_deprecation_warning(self, redis_docker):
        """A cache write against a real redis-py client warns about nothing.

        Mutation: restore client.setex(key, ttl, packed), which redis-py 8.x
        deprecates in favour of set(ex=...).
        Oracle: an empty DeprecationWarning list from the real client. A fake
        client cannot serve as the oracle here: it emits no warnings under
        any mutation, so the assertion would be unfalsifiable.
        """
        backend = cachu.get_backend('redis', ttl=300)
        backend.set('warm-up', 'value', 300)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            backend.set('warn-probe', 'value', 300)

        deprecations = [
            str(w.message) for w in caught
            if issubclass(w.category, DeprecationWarning)
        ]
        assert deprecations == []
