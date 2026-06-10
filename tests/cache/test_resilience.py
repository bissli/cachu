"""Tests for backend-failure resilience (fail-open read/lock/stat paths).

These reproduce the production incident where a transient Redis fault on a
cache read propagated out of the decorator and became a client-facing error.
A cache is an optimization: a briefly unreachable backend should degrade to a
miss (recompute from source), not fail the request.
"""
import cachu
import pytest
from cachu.backends.memory import MemoryBackend
from cachu.mutex import AsyncioMutex, ThreadingMutex


class TestSyncFailOpen:
    """Sync wrapper degrades to a miss on backend read/lock/stat errors.
    """

    def test_read_failure_falls_through_to_function(self, monkeypatch):
        """A backend read error degrades to a miss and the function still runs.
        """
        calls = []

        @cachu.cache(ttl=300, backend='memory')
        def func(x: int) -> int:
            calls.append(x)
            return x * 2

        assert func(5) == 10
        assert len(calls) == 1

        def boom(self, key):
            raise ConnectionError('backend down')

        monkeypatch.setattr(MemoryBackend, 'get_with_metadata', boom)

        assert func(5) == 10
        assert len(calls) == 2

    def test_mutex_acquire_failure_falls_through(self, monkeypatch):
        """A lock-acquire backend error degrades to running without the lock.
        """
        calls = []

        @cachu.cache(ttl=300, backend='memory')
        def func(x: int) -> int:
            calls.append(x)
            return x * 2

        def boom(self, timeout=None):
            raise ConnectionError('lock backend down')

        monkeypatch.setattr(ThreadingMutex, 'acquire', boom)

        assert func(5) == 10
        assert len(calls) == 1

    def test_stat_failure_does_not_break_request(self, monkeypatch):
        """A stats backend error never fails a successful call.
        """
        @cachu.cache(ttl=300, backend='memory')
        def func(x: int) -> int:
            return x * 2

        def boom(self, fn_name, stat):
            raise ConnectionError('stats down')

        monkeypatch.setattr(MemoryBackend, 'incr_stat', boom)

        assert func(5) == 10
        assert func(5) == 10

    def test_fail_closed_propagates_read_error(self, monkeypatch):
        """fail_open=False restores strict behavior: read errors propagate.
        """
        @cachu.cache(ttl=300, backend='memory')
        def func(x: int) -> int:
            return x * 2

        cachu.configure(fail_open=False)

        def boom(self, key):
            raise ConnectionError('backend down')

        monkeypatch.setattr(MemoryBackend, 'get_with_metadata', boom)

        with pytest.raises(ConnectionError):
            func(5)

    def test_stat_error_swallowed_even_when_fail_closed(self, monkeypatch):
        """Stats are best-effort: a stat error never propagates, even fail_open=False.
        """
        @cachu.cache(ttl=300, backend='memory')
        def func(x: int) -> int:
            return x * 2

        cachu.configure(fail_open=False)

        def boom(self, fn_name, stat):
            raise ConnectionError('stats down')

        monkeypatch.setattr(MemoryBackend, 'incr_stat', boom)

        assert func(5) == 10


class TestAsyncFailOpen:
    """Async wrapper degrades to a miss on backend read/lock/stat errors.
    """

    async def test_read_failure_falls_through_to_function(self, monkeypatch):
        """A backend read error degrades to a miss and the coroutine still runs.
        """
        calls = []

        @cachu.cache(ttl=300, backend='memory')
        async def func(x: int) -> int:
            calls.append(x)
            return x * 2

        assert await func(5) == 10
        assert len(calls) == 1

        async def boom(self, key):
            raise ConnectionError('backend down')

        monkeypatch.setattr(MemoryBackend, 'aget_with_metadata', boom)

        assert await func(5) == 10
        assert len(calls) == 2

    async def test_mutex_acquire_failure_falls_through(self, monkeypatch):
        """A lock-acquire backend error degrades to running without the lock.
        """
        calls = []

        @cachu.cache(ttl=300, backend='memory')
        async def func(x: int) -> int:
            calls.append(x)
            return x * 2

        async def boom(self, timeout=None):
            raise ConnectionError('lock backend down')

        monkeypatch.setattr(AsyncioMutex, 'acquire', boom)

        assert await func(5) == 10
        assert len(calls) == 1

    async def test_stat_failure_does_not_break_request(self, monkeypatch):
        """A stats backend error never fails a successful call.
        """
        @cachu.cache(ttl=300, backend='memory')
        async def func(x: int) -> int:
            return x * 2

        async def boom(self, fn_name, stat):
            raise ConnectionError('stats down')

        monkeypatch.setattr(MemoryBackend, 'aincr_stat', boom)

        assert await func(5) == 10
        assert await func(5) == 10

    async def test_fail_closed_propagates_read_error(self, monkeypatch):
        """fail_open=False restores strict behavior: read errors propagate.
        """
        @cachu.cache(ttl=300, backend='memory')
        async def func(x: int) -> int:
            return x * 2

        cachu.configure(fail_open=False)

        async def boom(self, key):
            raise ConnectionError('backend down')

        monkeypatch.setattr(MemoryBackend, 'aget_with_metadata', boom)

        with pytest.raises(ConnectionError):
            await func(5)
