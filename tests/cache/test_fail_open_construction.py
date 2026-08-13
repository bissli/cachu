"""Tests that fail_open also covers backend construction and mutex creation.

Notes
-----
- The decorator already degraded backend reads and lock acquisition to a
  miss; building the backend instance and building the per-key mutex were
  not covered.
- Both can construct a Redis client lazily, so a bad URL, a missing 'redis'
  extra or a DNS failure raised straight out of a decorated function
  despite fail_open=True.
- A caller that maps any exception to a failure - an auth gate answering
  401 - then turns a cache fault into an outage of the thing being cached.
"""
import cachu
import pytest
from cachu.manager import CacheManager

BAD_REDIS_URL = 'bogus://unreachable/0'


class TestSyncConstructionFailOpen:
    """A construction fault on the sync path degrades to an uncached call.
    """

    def test_mutex_construction_failure_returns_value(self):
        """A Redis client that cannot be built during get_mutex still answers the call.

        Mutation: drop the try/except around backend_inst.get_mutex().
        Oracle: the undecorated function's return value, 'v'.
        """
        cachu.configure(backend_default='redis', redis_url=BAD_REDIS_URL)

        @cachu.cache(ttl=60, tag='t')
        def fetch(k: str) -> str:
            return 'v'

        assert fetch('x') == 'v'

    def test_backend_construction_failure_returns_value(self, monkeypatch):
        """A backend that cannot be constructed at all still answers the call.

        Mutation: drop the try/except around manager.get_backend().
        Oracle: the undecorated function's return value, 42.
        """
        @cachu.cache(ttl=60, backend='memory')
        def fetch(k: str) -> int:
            return 42

        def boom(self, package, backend_type, ttl):
            raise RuntimeError('backend factory down')

        monkeypatch.setattr(CacheManager, '_create_backend', boom)

        assert fetch('x') == 42

    def test_function_runs_exactly_once_per_call_when_uncached(self, monkeypatch):
        """Falling through to an uncached call runs the function once, not twice.

        Mutation: fall through by continuing into the cache path instead of
        returning fn(...) directly, so the body runs again after the miss.
        Oracle: hand-counted invocation count, 2 for two calls.
        """
        calls = []

        @cachu.cache(ttl=60, backend='memory')
        def fetch(k: str) -> int:
            calls.append(k)
            return len(calls)

        def boom(self, package, backend_type, ttl):
            raise RuntimeError('backend factory down')

        monkeypatch.setattr(CacheManager, '_create_backend', boom)

        fetch('x')
        fetch('x')
        assert len(calls) == 2

    def test_fail_closed_still_propagates_construction_error(self, monkeypatch):
        """fail_open=False keeps strict behavior for construction faults too.

        Mutation: swallow the error unconditionally instead of honoring
        fail_open.
        Oracle: the sentinel error type raised by the stubbed factory.
        """
        @cachu.cache(ttl=60, backend='memory')
        def fetch(k: str) -> int:
            return 42

        cachu.configure(fail_open=False)

        def boom(self, package, backend_type, ttl):
            raise RuntimeError('backend factory down')

        monkeypatch.setattr(CacheManager, '_create_backend', boom)

        with pytest.raises(RuntimeError, match='backend factory down'):
            fetch('x')

    def test_fail_closed_still_propagates_mutex_error(self):
        """fail_open=False propagates a mutex construction fault.

        Mutation: swallow the mutex error unconditionally.
        Oracle: ValueError, which redis.from_url raises for an unknown scheme.
        """
        cachu.configure(
            backend_default='redis',
            redis_url=BAD_REDIS_URL,
            fail_open=False,
        )

        @cachu.cache(ttl=60, tag='t')
        def fetch(k: str) -> str:
            return 'v'

        with pytest.raises(ValueError):
            fetch('x')


class TestAsyncConstructionFailOpen:
    """A construction fault on the async path degrades to an uncached call.
    """

    async def test_mutex_construction_failure_returns_value(self):
        """An async mutex that cannot be built still answers the call.

        Mutation: drop the try/except around backend_inst.get_async_mutex().
        Oracle: the undecorated coroutine's return value, 'v'.
        """
        cachu.configure(backend_default='redis', redis_url=BAD_REDIS_URL)

        @cachu.cache(ttl=60, tag='t')
        async def fetch(k: str) -> str:
            return 'v'

        assert await fetch('x') == 'v'

    async def test_backend_construction_failure_returns_value(self, monkeypatch):
        """An async backend that cannot be constructed still answers the call.

        Mutation: drop the try/except around manager.aget_backend().
        Oracle: the undecorated coroutine's return value, 42.
        """
        @cachu.cache(ttl=60, backend='memory')
        async def fetch(k: str) -> int:
            return 42

        def boom(self, package, backend_type, ttl):
            raise RuntimeError('backend factory down')

        monkeypatch.setattr(CacheManager, '_create_backend', boom)

        assert await fetch('x') == 42

    async def test_fail_closed_still_propagates_construction_error(self, monkeypatch):
        """fail_open=False keeps strict behavior on the async path.

        Mutation: swallow the error unconditionally instead of honoring
        fail_open.
        Oracle: the sentinel error type raised by the stubbed factory.
        """
        @cachu.cache(ttl=60, backend='memory')
        async def fetch(k: str) -> int:
            return 42

        cachu.configure(fail_open=False)

        def boom(self, package, backend_type, ttl):
            raise RuntimeError('backend factory down')

        monkeypatch.setattr(CacheManager, '_create_backend', boom)

        with pytest.raises(RuntimeError, match='backend factory down'):
            await fetch('x')


class _Exploding:
    """Stand-in for a lazy proxy whose attribute access raises.

    Notes
    -----
    - Cache key generation probes candidate values for `driver_connection`,
      `dialect` and `engine` to skip database connections, and renders the
      survivors with `repr`. A SQLAlchemy lazy load or a Django
      SimpleLazyObject can raise from either step on a perfectly healthy
      backend.
    """

    def __getattr__(self, name):
        raise RuntimeError('lazy load failed')

    def __repr__(self):
        raise RuntimeError('repr failed')


class TestKeyGenerationFailOpen:
    """A cache key that cannot be built is a cache fault like any other.
    """

    def test_unrenderable_argument_still_returns_a_value(self):
        """A raising argument degrades to an uncached call, not an error.

        Mutation: drop the try/except around key_generator(), which lets a
        healthy backend still fail the request - the exact promise BUGS.md
        called dangerous, since fail_open reads as "the cache can only cost
        speed".
        Oracle: the undecorated function's return value, 'allow'.
        """
        @cachu.cache(ttl=60, backend='memory', tag='keys')
        def lookup(subject) -> str:
            return 'allow'

        assert lookup(_Exploding()) == 'allow'

    def test_function_still_runs_exactly_once(self):
        """The uncached fallthrough does not double-invoke the function.

        Mutation: fall through into the cache path instead of returning
        directly, so the body runs again after the miss.
        Oracle: hand-counted invocation count, 1.
        """
        calls = []

        @cachu.cache(ttl=60, backend='memory', tag='keys')
        def lookup(subject) -> str:
            calls.append(1)
            return 'allow'

        lookup(_Exploding())

        assert len(calls) == 1

    def test_fail_closed_propagates_the_key_error(self):
        """fail_open=False surfaces a key-generation fault.

        Mutation: swallow it unconditionally.
        Oracle: the sentinel error raised by the exploding argument.
        """
        @cachu.cache(ttl=60, backend='memory', tag='keys')
        def lookup(subject) -> str:
            return 'allow'

        cachu.configure(fail_open=False)

        with pytest.raises(RuntimeError, match='lazy load failed'):
            lookup(_Exploding())

    async def test_async_unrenderable_argument_still_returns_a_value(self):
        """The async path degrades the same way.

        Mutation: guard only the sync wrapper.
        Oracle: the undecorated coroutine's return value, 'allow'.
        """
        @cachu.cache(ttl=60, backend='memory', tag='keys')
        async def lookup(subject) -> str:
            return 'allow'

        assert await lookup(_Exploding()) == 'allow'
