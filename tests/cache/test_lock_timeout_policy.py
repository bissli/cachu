"""Tests for on_lock_timeout, the policy for a waiter that misses the mutex.

Notes
-----
- When mutex.acquire() returns False the wrapper historically ran the
  function anyway, so lowering lock_timeout to shed load had the opposite
  effect: each waiter that gave up became its own backing-store read.
- 'run' keeps that behaviour; 'raise' lets a caller shed load instead.
"""
import threading
import time

import cachu
import pytest
from cachu.backends.memory import MemoryBackend
from cachu.exception import CacheLockTimeout
from cachu.mutex import AsyncioMutex, ThreadingMutex


@pytest.fixture
def lock_always_times_out(monkeypatch):
    """Make every dogpile acquire report a clean timeout.
    """
    monkeypatch.setattr(ThreadingMutex, 'acquire', lambda self, timeout=None: False)


class TestRunIsTheDefault:
    """The historical behaviour is untouched unless opted out of.
    """

    def test_default_config_runs_on_timeout(self):
        """on_lock_timeout defaults to 'run'.

        Mutation: default to 'raise', which would start raising in every
        existing deployment that contends on a key.
        Oracle: the documented default string, 'run'.
        """
        assert cachu.CacheConfig().on_lock_timeout == 'run'

    def test_timed_out_waiter_still_returns_a_value(self, lock_always_times_out):
        """With 'run', a waiter that misses the lock executes the function.

        Mutation: raise unconditionally on a lock timeout.
        Oracle: the undecorated function's return value, 10.
        """
        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            return key * 2

        assert fetch(5) == 10


class TestRaiseShedsLoad:
    """'raise' converts a lock timeout into a typed error.
    """

    def test_timed_out_waiter_raises(self, lock_always_times_out):
        """A waiter that misses the lock raises instead of stampeding.

        Mutation: invert the on_lock_timeout comparison, or drop the raise.
        Oracle: CacheLockTimeout, the documented load-shedding signal.
        """
        cachu.configure(on_lock_timeout='raise')

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            return key * 2

        with pytest.raises(CacheLockTimeout, match='fetch'):
            fetch(5)

    def test_function_is_not_executed_when_raising(self, lock_always_times_out):
        """Shedding load means the backing store is never touched.

        Mutation: raise after calling the function, which sheds no load at all.
        Oracle: hand-counted invocation count, 0.
        """
        cachu.configure(on_lock_timeout='raise')
        calls = []

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            calls.append(key)
            return key * 2

        with pytest.raises(CacheLockTimeout):
            fetch(5)

        assert calls == []

    def test_cached_value_wins_over_raising(self, monkeypatch):
        """A waiter whose wait was rewarded returns the value, it does not raise.

        Mutation: move the raise above the post-lock re-read, so a waiter that
        the lock holder just served would fail instead of being served.
        Oracle: the value the lock holder stored, 10.
        """
        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            return key * 2

        assert fetch(5) == 10

        cachu.configure(on_lock_timeout='raise')
        monkeypatch.setattr(ThreadingMutex, 'acquire', lambda self, timeout=None: False)

        assert fetch(5) == 10

    def test_error_is_a_cache_error(self, lock_always_times_out):
        """CacheLockTimeout stays inside the library's exception hierarchy.

        Mutation: raise a bare TimeoutError, which callers catching CacheError
        would miss.
        Oracle: cachu.CacheError, the documented base class.
        """
        cachu.configure(on_lock_timeout='raise')

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            return key * 2

        with pytest.raises(cachu.CacheError):
            fetch(5)


class TestSheddingSurvivesADeadline:
    """`on_lock_timeout='raise'` keeps shedding when cache_deadline is set.

    Notes
    -----
    - `_remaining_lock_wait` clamps the wait to what is left of the budget,
      so with `cache_deadline <= lock_timeout` the wait is always shorter
      than `lock_timeout`.
    - Discriminating a "real" timeout by comparing the two made the raise
      dead code in exactly that configuration, while the clamp still cut the
      wait short - so every waiter gave up early and ran the function. The
      two documented remedies for slow backends and for stampedes silently
      cancelled each other out.
    """

    @pytest.mark.parametrize('deadline', [0.5, 1.0, 5.0])
    def test_raise_still_sheds_at_any_deadline(self, deadline, lock_always_times_out):
        """Shedding does not depend on how cache_deadline compares to lock_timeout.

        Mutation: gate the raise on `lock_wait >= cfg.lock_timeout`, which is
        never true once a deadline has clamped the wait.
        Oracle: CacheLockTimeout, at deadlines below, equal to and above the
        1.0s lock_timeout.
        """
        cachu.configure(
            on_lock_timeout='raise', lock_timeout=1.0, cache_deadline=deadline)

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            return key * 2

        with pytest.raises(CacheLockTimeout):
            fetch(5)

    def test_an_exhausted_budget_sheds_rather_than_stampedes(self, monkeypatch):
        """A budget spent before the lock is even attempted still sheds.

        Mutation: treat "never attempted the lock" as "not a timeout", so a
        deadline-exhausted caller runs the function - a stampede under the
        one setting whose purpose is to prevent one.
        Oracle: CacheLockTimeout, with the function never invoked.
        """
        cachu.configure(on_lock_timeout='raise', cache_deadline=0.01)
        calls = []

        def slow_read(self, key):
            time.sleep(0.05)
            return cachu.api.NO_VALUE, None

        monkeypatch.setattr(MemoryBackend, 'get_with_metadata', slow_read)

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            calls.append(key)
            return key * 2

        with pytest.raises(CacheLockTimeout):
            fetch(5)

        assert calls == []


class TestFailOpenIsNotOverridden:
    """A lock backend *error* is not a lock *timeout*.
    """

    def test_acquire_error_does_not_raise_lock_timeout(self, monkeypatch):
        """A raising acquire still degrades to running without the lock.

        Mutation: set lock_timed_out = True in the acquire except-branch,
        which would convert every fail_open lock fault into a hard error.
        Oracle: the undecorated function's return value, 10.
        """
        cachu.configure(on_lock_timeout='raise')

        def boom(self, timeout=None):
            raise ConnectionError('lock backend down')

        monkeypatch.setattr(ThreadingMutex, 'acquire', boom)

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            return key * 2

        assert fetch(5) == 10

    def test_missing_mutex_does_not_raise_lock_timeout(self, monkeypatch):
        """A mutex that could not be constructed is not a lock timeout either.

        Mutation: treat a None mutex as a timeout.
        Oracle: the undecorated function's return value, 10.
        """
        cachu.configure(on_lock_timeout='raise')

        def boom(self, key):
            raise ConnectionError('mutex backend down')

        monkeypatch.setattr(MemoryBackend, 'get_mutex', boom)

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            return key * 2

        assert fetch(5) == 10


class TestAsyncPolicy:
    """The async wrapper honours the same policy.
    """

    async def test_async_timed_out_waiter_raises(self, monkeypatch):
        """'raise' applies to async callers too.

        Mutation: implement the policy only in the sync wrapper.
        Oracle: CacheLockTimeout, the documented load-shedding signal.
        """
        cachu.configure(on_lock_timeout='raise')

        async def never(self, timeout=None):
            return False

        monkeypatch.setattr(AsyncioMutex, 'acquire', never)

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        async def fetch(key: int) -> int:
            return key * 2

        with pytest.raises(CacheLockTimeout, match='fetch'):
            await fetch(5)

    async def test_async_default_still_runs(self, monkeypatch):
        """The async default remains 'run'.

        Mutation: default the async path to 'raise'.
        Oracle: the undecorated coroutine's return value, 10.
        """
        async def never(self, timeout=None):
            return False

        monkeypatch.setattr(AsyncioMutex, 'acquire', never)

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        async def fetch(key: int) -> int:
            return key * 2

        assert await fetch(5) == 10


class TestRealContention:
    """End-to-end: real threads, a real mutex, a real slow store.
    """

    def test_raise_keeps_store_reads_at_one(self):
        """Under contention only the lock holder reads the store.

        Mutation: keep the stampede behaviour ('run'), which the report
        measured at 6 store reads for 6 concurrent same-key callers.
        Oracle: hand-derived read count, 1 - exactly the lock holder.
        """
        cachu.configure(on_lock_timeout='raise', lock_timeout=0.05)

        reads = []
        holder_is_inside = threading.Event()
        release_holder = threading.Event()

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            reads.append(key)
            holder_is_inside.set()
            release_holder.wait(timeout=10.0)
            return key * 2

        holder = threading.Thread(target=fetch, args=(7,))
        holder.start()
        assert holder_is_inside.wait(timeout=10.0)

        outcomes = []

        def waiter():
            try:
                outcomes.append(fetch(7))
            except CacheLockTimeout:
                outcomes.append('shed')

        waiters = [threading.Thread(target=waiter) for _ in range(5)]
        for thread in waiters:
            thread.start()
        for thread in waiters:
            thread.join(timeout=10.0)

        release_holder.set()
        holder.join(timeout=10.0)

        assert len(reads) == 1
        assert outcomes == ['shed'] * 5


class TestValidation:
    """Only the documented policies are accepted.
    """

    @pytest.mark.parametrize('bad', ['wait', 'RAISE', '', 0])
    def test_unknown_policy_is_rejected(self, bad):
        """An unrecognised policy raises rather than silently meaning 'run'.

        Mutation: skip validation, so a typo like 'Raise' degrades to the
        stampede the caller was trying to avoid.
        Oracle: ValueError, the documented rejection for invalid config.
        """
        with pytest.raises(ValueError, match='on_lock_timeout'):
            cachu.configure(on_lock_timeout=bad)
