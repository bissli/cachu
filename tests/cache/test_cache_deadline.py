"""Tests for cache_deadline, the total budget for cache work in one call.

Notes
-----
- Redis timeout budgets compound rather than add: socket_timeout applies to
  connect and read, redis-py retries it, and one cached call performs
  roughly five Redis operations, one of which busy-loops until lock_timeout.
- Against a blackholed endpoint a single cached call was measured at 100.7
  seconds. It returned the right answer via fail_open, but a caller with a
  deadline cannot tell that from an outage - and because it is a hang rather
  than an exception, try/except cannot shorten it.
- Time is driven by an injected monotonic clock rather than real sleeps, so
  the budget arithmetic is asserted exactly instead of approximately.
"""
import logging

import cachu
import pytest
from cachu import decorator as decorator_module
from cachu.backends.memory import MemoryBackend
from cachu.manager import CacheManager
from cachu.mutex import AsyncioMutex, ThreadingMutex


class _Clock:
    """Monotonic clock stand-in that only advances when told to.
    """

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _SlowBackend(MemoryBackend):
    """Memory backend that charges the injected clock for every operation.

    Stands in for a Redis endpoint whose socket budgets dominate the call:
    every cache operation costs `cost` seconds of monotonic time and is
    recorded in `ops`, so the exact sequence of cache work a call performed
    can be asserted.
    """

    def __init__(self, clock: _Clock, cost: float) -> None:
        super().__init__()
        self._clock = clock
        self._cost = cost
        self.ops: list[str] = []

    def _charge(self, op: str) -> None:
        self.ops.append(op)
        self._clock.advance(self._cost)

    def get_with_metadata(self, key):
        self._charge('get')
        return super().get_with_metadata(key)

    def set(self, key, value, ttl):
        self._charge('set')
        super().set(key, value, ttl)

    def incr_stat(self, fn_name, stat):
        self._charge(f'stat:{stat}')
        super().incr_stat(fn_name, stat)

    async def aget_with_metadata(self, key):
        self._charge('aget')
        return await super().aget_with_metadata(key)

    async def aset(self, key, value, ttl):
        self._charge('aset')
        await super().aset(key, value, ttl)

    async def aincr_stat(self, fn_name, stat):
        self._charge(f'astat:{stat}')
        await super().aincr_stat(fn_name, stat)


@pytest.fixture
def clock(monkeypatch):
    """Replace the decorator module's time source with a controllable clock.
    """
    fake = _Clock()
    monkeypatch.setattr(decorator_module, 'time', fake)
    return fake


def _install(monkeypatch, backend):
    """Force the manager to hand out the given backend instance.
    """
    monkeypatch.setattr(
        CacheManager,
        '_create_backend',
        lambda self, package, backend_type, ttl: backend,
    )


class TestBudgetStopsCacheWork:
    """An exhausted budget skips the remaining cache steps.
    """

    def test_slow_backend_stops_costing_time_after_the_deadline(self, clock, monkeypatch):
        """Cache work stops once the budget is spent, bounding the wall clock.

        Mutation: drop the _budget_spent() guard before the write, so a wedged
        backend is paid for on every step of every call.
        Oracle: hand-computed cost - two 0.6s reads spend the 1.0s budget, so
        the miss-stat and the write must not run, leaving ops == ['get', 'get'].
        """
        backend = _SlowBackend(clock, cost=0.6)
        _install(monkeypatch, backend)
        cachu.configure(cache_deadline=1.0)

        @cachu.cache(ttl=300, backend='memory', tag='slow')
        def fetch(key: str) -> str:
            return 'value'

        assert fetch('k') == 'value'
        assert backend.ops == ['get', 'get']
        assert clock.now == pytest.approx(1001.2)

    def test_skipped_write_leaves_the_entry_uncached(self, clock, monkeypatch):
        """A write skipped for budget really did not happen.

        Mutation: log the skip but perform the write anyway.
        Oracle: hand-counted invocation count, 2 - the second call must miss
        because nothing was stored.
        """
        backend = _SlowBackend(clock, cost=0.6)
        _install(monkeypatch, backend)
        cachu.configure(cache_deadline=1.0)

        calls = []

        @cachu.cache(ttl=300, backend='memory', tag='slow')
        def fetch(key: str) -> str:
            calls.append(key)
            return 'value'

        fetch('k')
        fetch('k')

        assert len(calls) == 2

    def test_caller_still_gets_the_correct_value(self, clock, monkeypatch):
        """Shedding cache work never changes the answer, and really sheds it.

        Mutation: delete any of the post-read _budget_spent guards - the
        post-lock re-read, the miss stat, or the write. One read already
        overruns the budget 200-fold, so every later step must be skipped.
        Oracle: the undecorated return value plus the exact cache work
        performed, which is one read and nothing else.
        """
        backend = _SlowBackend(clock, cost=99.0)
        _install(monkeypatch, backend)
        cachu.configure(cache_deadline=0.5)

        seen = []
        original = ThreadingMutex.acquire

        def record(self, timeout=None):
            seen.append(timeout)
            return original(self, timeout)

        monkeypatch.setattr(ThreadingMutex, 'acquire', record)

        @cachu.cache(ttl=300, backend='memory', tag='slow')
        def fetch(key: str) -> str:
            return 'value'

        assert fetch('k') == 'value'
        assert backend.ops == ['get']
        assert seen == []

    def test_budget_is_spent_at_exactly_the_deadline(self, clock, monkeypatch):
        """The budget comparison is inclusive at the boundary.

        Mutation: `>=` -> `>` in _budget_spent, which lets one more cache
        step through at exactly the deadline.
        Oracle: hand-computed arithmetic - two 0.5s reads spend a 1.0s budget
        exactly, so the miss stat and the write must not run.
        """
        backend = _SlowBackend(clock, cost=0.5)
        _install(monkeypatch, backend)
        cachu.configure(cache_deadline=1.0)

        @cachu.cache(ttl=300, backend='memory', tag='slow')
        def fetch(key: str) -> str:
            return 'value'

        fetch('k')

        assert clock.now == pytest.approx(1001.0)
        assert backend.ops == ['get', 'get']

    def test_lock_wait_is_clamped_to_the_remaining_budget(self, clock, monkeypatch):
        """The dogpile wait cannot outlive the budget.

        Mutation: pass cfg.lock_timeout straight through instead of clamping,
        restoring the unbounded wait the report measured.
        Oracle: hand-computed remainder - 1.0s budget minus a 0.4s read leaves
        0.6s, which is below the 10.0s lock_timeout.
        """
        backend = _SlowBackend(clock, cost=0.4)
        _install(monkeypatch, backend)
        cachu.configure(cache_deadline=1.0, lock_timeout=10.0)

        seen = []
        original = ThreadingMutex.acquire

        def record(self, timeout=None):
            seen.append(timeout)
            return original(self, timeout)

        monkeypatch.setattr(ThreadingMutex, 'acquire', record)

        @cachu.cache(ttl=300, backend='memory', tag='slow')
        def fetch(key: str) -> str:
            return 'value'

        fetch('k')

        assert seen == [pytest.approx(0.6)]


class TestFunctionTimeIsNotCharged:
    """Only cache work spends the budget; the decorated function does not.
    """

    def test_slow_function_is_still_cached(self, clock, monkeypatch):
        """A function slower than the deadline still gets its result stored.

        Mutation: remove the `started += time.monotonic() - fn_started`
        compensation, so any function slower than cache_deadline would never
        populate the cache.
        Oracle: hand-counted invocation count, 1 - the second call must hit.
        """
        backend = _SlowBackend(clock, cost=0.01)
        _install(monkeypatch, backend)
        cachu.configure(cache_deadline=1.0)

        calls = []

        @cachu.cache(ttl=300, backend='memory', tag='slow')
        def fetch(key: str) -> str:
            calls.append(key)
            clock.advance(100.0)
            return 'value'

        fetch('k')
        assert fetch('k') == 'value'
        assert len(calls) == 1
        assert 'set' in backend.ops


class TestAsyncMirrorsSync:
    """Every budget guard exists on the async path too.

    Notes
    -----
    - The two wrappers are near-identical by necessity, so a guard added to
      one and forgotten in the other is the most likely defect here.
    """

    async def test_async_slow_function_is_still_cached(self, clock, monkeypatch):
        """An async function slower than the deadline still populates the cache.

        Mutation: remove `started += time.monotonic() - fn_started` from the
        ASYNC wrapper only. Its sync twin is covered; without this, any async
        function slower than cache_deadline would never cache at all.
        Oracle: hand-counted invocation count, 1 - the second call must hit.
        """
        backend = _SlowBackend(clock, cost=0.01)
        _install(monkeypatch, backend)
        cachu.configure(cache_deadline=1.0)

        calls = []

        @cachu.cache(ttl=300, backend='memory', tag='slow')
        async def fetch(key: str) -> str:
            calls.append(key)
            clock.advance(100.0)
            return 'value'

        await fetch('k')
        assert await fetch('k') == 'value'
        assert len(calls) == 1
        assert 'aset' in backend.ops

    async def test_async_exhausted_budget_skips_every_later_step(self, clock, monkeypatch):
        """An exhausted budget stops async cache work, mirroring the sync path.

        Mutation: delete the async mutex-block guard, the async post-lock
        read guard, or the async hit-stat guard.
        Oracle: the exact cache work performed - one read and nothing else.
        """
        backend = _SlowBackend(clock, cost=99.0)
        _install(monkeypatch, backend)
        cachu.configure(cache_deadline=0.5)

        seen = []

        async def record(self, timeout=None):
            seen.append(timeout)
            return True

        monkeypatch.setattr(AsyncioMutex, 'acquire', record)

        @cachu.cache(ttl=300, backend='memory', tag='slow')
        async def fetch(key: str) -> str:
            return 'value'

        assert await fetch('k') == 'value'
        assert backend.ops == ['aget']
        assert seen == []

    async def test_async_hit_stat_is_skipped_when_the_budget_is_spent(self, clock, monkeypatch):
        """A hit reached on an exhausted budget does not pay for a stat write.

        Mutation: delete the _budget_spent guard around the hit stat, on
        either wrapper. The read succeeded, but it may have consumed the
        whole budget on the way.
        Oracle: the cached value plus the exact cache work performed - one
        read that HITS, and no stat write after it.
        """
        backend = _SlowBackend(clock, cost=0.01)
        _install(monkeypatch, backend)
        cachu.configure(cache_deadline=0.5)

        @cachu.cache(ttl=300, backend='memory', tag='slow')
        async def fetch(key: str) -> str:
            return 'value'

        assert await fetch('k') == 'value'
        assert 'aset' in backend.ops

        backend._cost = 99.0
        backend.ops.clear()

        assert await fetch('k') == 'value'

        assert backend.ops == ['aget']


class TestDefaultIsUnbounded:
    """No deadline configured means no behaviour change at all.
    """

    def test_without_deadline_every_cache_step_runs(self, clock, monkeypatch):
        """The unconfigured default performs the full read/stat/write sequence.

        Mutation: default cache_deadline to a finite value.
        Oracle: the documented per-call sequence - read, miss stat, write.
        """
        backend = _SlowBackend(clock, cost=50.0)
        _install(monkeypatch, backend)

        @cachu.cache(ttl=300, backend='memory', tag='slow')
        def fetch(key: str) -> str:
            return 'value'

        fetch('k')

        assert backend.ops == ['get', 'get', 'stat:misses', 'set']

    def test_default_config_has_no_deadline(self):
        """cache_deadline is opt-in.

        Mutation: ship a finite default, silently truncating existing users'
        cache work.
        Oracle: None, the documented unbounded sentinel.
        """
        assert cachu.CacheConfig().cache_deadline is None


class TestAsyncBudget:
    """The async wrapper enforces the same budget.
    """

    async def test_async_write_is_skipped_when_budget_is_spent(self, clock, monkeypatch):
        """An exhausted budget skips the async write too.

        Mutation: guard only the sync path, leaving async callers unbounded.
        Oracle: hand-counted invocation count, 2 - the second call must miss.
        """
        backend = _SlowBackend(clock, cost=0.6)
        _install(monkeypatch, backend)
        cachu.configure(cache_deadline=1.0)

        calls = []

        @cachu.cache(ttl=300, backend='memory', tag='slow')
        async def fetch(key: str) -> str:
            calls.append(key)
            return 'value'

        assert await fetch('k') == 'value'
        await fetch('k')

        assert len(calls) == 2
        assert backend.ops == ['aget', 'aget', 'aget', 'aget']


class TestUnenforceableDeadlineIsReported:
    """A deadline the Redis budgets cannot honour is called out, not silently
    patched over.

    Notes
    -----
    - The deadline is only checked between steps, so a call already blocked
      in a socket read runs to completion. redis-py retries inside a single
      operation, so that call can cost socket_timeout * (1 + retry_count) -
      20s on the shipped defaults, which swamps a 1s deadline.
    - Deriving the socket timeout from the deadline instead was measured to
      be worse: on a healthy but slow endpoint every read and write timed
      out, turning the cache into a 100% miss that fail_open then hid.
    """

    def test_impossible_deadline_warns_with_the_arithmetic(self, caplog):
        """Configuring a deadline the socket budget dwarfs logs the numbers.

        Mutation: drop the warning, leaving a caller believing a 1s deadline
        bought a 1s cache when the floor is 20s.
        Oracle: hand-computed floor - 5.0 * (1 + 3) = 20 - present in the
        logged message.
        """
        cachu.configure(
            backend_default='redis',
            cache_deadline=1.0,
            redis_socket_timeout=5.0,
            redis_retry_count=3,
        )

        with caplog.at_level(logging.WARNING, logger='cachu.manager'):
            cachu.get_backend('redis', ttl=300)

        messages = [r.message for r in caplog.records]
        assert any('cannot be honoured' in m and '20s' in m for m in messages)

    def test_configured_socket_timeout_is_never_overridden(self, caplog):
        """A deadline does not rewrite an explicitly configured socket timeout.

        Mutation: derive socket_timeout from cache_deadline. That silently
        contradicts the value get_config() reports and, on a slow endpoint,
        times out every operation.
        Oracle: the configured value, 9.0, unchanged on the live client.
        """
        cachu.configure(
            backend_default='redis',
            cache_deadline=1.0,
            redis_socket_timeout=9.0,
            redis_retry_count=3,
        )

        backend = cachu.get_backend('redis', ttl=300)

        assert backend._socket_timeout == pytest.approx(9.0)
        assert cachu.get_config().redis_socket_timeout == pytest.approx(9.0)

    def test_an_achievable_deadline_does_not_warn(self, caplog):
        """Budgets that already fit the deadline produce no noise.

        Mutation: warn unconditionally whenever cache_deadline is set,
        training callers to ignore the message.
        Oracle: hand-computed floor - 0.2 * (1 + 1) = 0.4, which is under
        the 1.0s deadline.
        """
        cachu.configure(
            backend_default='redis',
            cache_deadline=1.0,
            redis_socket_timeout=0.2,
            redis_retry_count=1,
        )

        with caplog.at_level(logging.WARNING, logger='cachu.manager'):
            cachu.get_backend('redis', ttl=300)

        assert [r for r in caplog.records if 'cannot be honoured' in r.message] == []

    def test_no_deadline_never_warns(self, caplog):
        """Callers who set no deadline are not warned about timeouts.

        Mutation: compare against a default deadline instead of None.
        Oracle: an empty warning list for the shipped defaults.
        """
        cachu.configure(backend_default='redis', redis_socket_timeout=5.0)

        with caplog.at_level(logging.WARNING, logger='cachu.manager'):
            backend = cachu.get_backend('redis', ttl=300)

        assert backend._socket_timeout == pytest.approx(5.0)
        assert [r for r in caplog.records if 'cannot be honoured' in r.message] == []


class TestValidation:
    """cache_deadline rejects values that would silently disable caching.
    """

    @pytest.mark.parametrize('bad', [0, -1.0, 'soon', True])
    def test_invalid_deadline_is_rejected(self, bad):
        """A non-positive or non-numeric deadline raises at configure time.

        Mutation: accept 0, which would skip every cache step on every call
        while looking like a valid setting.
        Oracle: ValueError, the documented rejection for invalid config.
        """
        with pytest.raises(ValueError, match='cache_deadline'):
            cachu.configure(cache_deadline=bad)
