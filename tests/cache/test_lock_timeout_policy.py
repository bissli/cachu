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

        Mutation: move the raise above the post-lock re-read, so a waiter
        that the lock holder just served would fail instead of being served.
        Oracle: the value the holder stored, 10, reached only by the
        post-lock read. The pre-lock read must MISS, otherwise the wrapper
        returns before the mutex block and the mutation is invisible - so
        the stub answers NO_VALUE first and the stored value second, which
        is exactly what a waiter observes when the holder writes while it
        waits.
        """
        cachu.configure(on_lock_timeout='raise')
        reads = []

        def arriving_late(self, key):
            reads.append(key)
            if len(reads) == 1:
                return cachu.api.NO_VALUE, None
            return 10, time.time()

        monkeypatch.setattr(MemoryBackend, 'get_with_metadata', arriving_late)
        monkeypatch.setattr(ThreadingMutex, 'acquire', lambda self, timeout=None: False)

        calls = []

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            calls.append(key)
            return -1

        assert fetch(5) == 10
        assert len(reads) == 2
        assert calls == []

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
    """`on_lock_timeout='raise'` sheds on contention, and only on contention.

    Notes
    -----
    - The two settings are orthogonal: `lock_timeout` bounds waiting for
      another caller's function, `cache_deadline` bounds cachu's own I/O.
      Shedding therefore has to survive any deadline, and must not be
      triggered by one.
    - Both halves have been wrong in the past. Discriminating a "real"
      timeout by comparing the wait against `lock_timeout` made the raise
      dead code whenever a deadline was set; treating a spent budget as a
      timeout then sheds callers no lock ever contended, which is a total
      outage rather than load shedding.
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

    def test_an_exhausted_budget_runs_rather_than_shedding(self, monkeypatch):
        """A lock never attempted is not a lock timeout.

        Mutation: treat "budget spent before the acquire" as a timeout, as
        0.4.1 did. Shedding then needs no contention at all: a cache merely
        slower than the budget sheds every caller, so the function never
        runs, so nothing is ever stored, so nothing recovers - a permanent
        outage of the decorated call under fail_open=True.
        Oracle: the function's own return value, and a call count proving it
        ran - with a 0.05s read against a 0.01s budget, which is exactly the
        state that used to raise.
        """
        cachu.configure(on_lock_timeout='raise', cache_deadline=0.01)
        calls = []
        attempts = []

        def slow_read(self, key):
            time.sleep(0.05)
            return cachu.api.NO_VALUE, None

        original_acquire = ThreadingMutex.acquire

        def record(self, timeout=None):
            attempts.append(timeout)
            return original_acquire(self, timeout)

        monkeypatch.setattr(MemoryBackend, 'get_with_metadata', slow_read)
        monkeypatch.setattr(ThreadingMutex, 'acquire', record)

        @cachu.cache(ttl=300, backend='memory', tag='herd')
        def fetch(key: int) -> int:
            calls.append(key)
            return key * 2

        assert fetch(5) == 10
        assert calls == [5]
        assert attempts == []


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
