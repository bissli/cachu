"""Tests that cache_clear reaches regions in a process that has not cached yet.

Notes
-----
- cache_clear iterated only backends already instantiated in this process,
  and a backend is first built inside the first decorated call.
- A clear performed before any cached call therefore touched nothing and
  returned 0 - indistinguishable from "there was nothing to clear".
- In tests this is silent and dangerous: a fixture that clears at setup does
  nothing, and against a shared backend the test is then served a value
  cached by a previous run. The reported incident was a test asserting an
  auth denial that passed while the underlying lookup was entirely broken.
- @cache now registers its (package, backend, ttl) region at decoration
  time, so cache_clear can materialize and really clear it.
"""
import logging

import cachu
import pytest
from cachu.manager import manager


@pytest.fixture
def cold_process(temp_cache_dir):
    """Drop every backend instance, keeping the on-disk state a restart keeps.
    """
    def restart() -> None:
        manager.clear()
    return restart


class TestColdClearReachesSharedState:
    """A clear before any cached call really clears a persistent backend.
    """

    def test_file_backend_entry_is_cleared_after_a_restart(self, cold_process):
        """A SQLite-backed entry does not survive a cold cache_clear.

        Mutation: remove the manager.materialize() call from cache_clear,
        restoring the silent no-op that served the stale entry.
        Oracle: hand-counted invocation count, 2 - the post-clear call must
        recompute rather than read the previous run's value.
        """
        calls = []

        @cachu.cache(ttl=300, backend='file', tag='authz')
        def lookup(user: str) -> str:
            calls.append(user)
            return f'allow:{user}'

        assert lookup('ana') == 'allow:ana'
        assert len(calls) == 1

        cold_process()

        assert cachu.cache_clear(tag='authz') == 1
        assert lookup('ana') == 'allow:ana'
        assert len(calls) == 2

    def test_cold_clear_reports_what_it_cleared(self, cold_process):
        """The returned count reflects entries actually removed.

        Mutation: return 0 unconditionally when nothing was instantiated.
        Oracle: hand-counted entry count, 3 distinct keys written.
        """
        @cachu.cache(ttl=300, backend='file', tag='authz')
        def lookup(user: str) -> str:
            return f'allow:{user}'

        for user in ('ana', 'bo', 'cy'):
            lookup(user)

        cold_process()

        assert cachu.cache_clear(tag='authz') == 3

    def test_tag_scoping_survives_materialization(self, cold_process):
        """Materializing extra regions does not widen the clear.

        Mutation: ignore the tag pattern once regions are materialized.
        Oracle: hand-derived survivor - the untagged cache still hits, so its
        function runs once across both calls.
        """
        tagged_calls = []
        other_calls = []

        @cachu.cache(ttl=300, backend='file', tag='authz')
        def tagged(user: str) -> str:
            tagged_calls.append(user)
            return user

        @cachu.cache(ttl=300, backend='file', tag='docs')
        def untagged(user: str) -> str:
            other_calls.append(user)
            return user

        tagged('ana')
        untagged('ana')

        cold_process()
        cachu.cache_clear(tag='authz')

        tagged('ana')
        untagged('ana')

        assert len(tagged_calls) == 2
        assert len(other_calls) == 1

    async def test_async_cold_clear_reaches_the_region(self, cold_process):
        """async_cache_clear materializes its regions too.

        Mutation: implement materialization only in the sync clear.
        Oracle: hand-counted invocation count, 2 - the post-clear call must
        recompute.
        """
        calls = []

        @cachu.cache(ttl=300, backend='file', tag='authz')
        async def lookup(user: str) -> str:
            calls.append(user)
            return f'allow:{user}'

        await lookup('ana')
        cold_process()

        assert await cachu.async_cache_clear(tag='authz') == 1

        await lookup('ana')
        assert len(calls) == 2


class TestRegionRegistry:
    """Regions are known from decoration time, before any call runs.
    """

    def test_decoration_alone_registers_the_region(self):
        """Defining a cached function is enough to make its region findable.

        Mutation: register the region on first call instead of at decoration,
        which is exactly the cold-process hole.
        Oracle: the region tuple derived from the decorator arguments.
        """
        package = cachu.config._get_caller_package()

        @cachu.cache(ttl=1234, backend='memory', tag='fresh')
        def never_called(x: int) -> int:
            return x

        assert (package, 'memory', 1234) in manager.get_regions(package)

    def test_dynamic_ttl_registers_the_dynamic_region(self):
        """A callable ttl registers the -1 region the backend actually uses.

        Mutation: register the callable itself, or 300, instead of -1.
        Oracle: -1, the documented dynamic-TTL region identifier.
        """
        package = cachu.config._get_caller_package()

        @cachu.cache(ttl=lambda result: 60, backend='memory', tag='dyn')
        def dynamic(x: int) -> int:
            return x

        assert (package, 'memory', -1) in manager.get_regions(package)

    def test_regions_are_filtered_by_backend_and_ttl(self):
        """get_regions narrows on both backend type and ttl.

        Mutation: ignore one of the filters, over-materializing on every
        clear.
        Oracle: hand-derived membership for the two declared regions.
        """
        package = cachu.config._get_caller_package()

        @cachu.cache(ttl=111, backend='memory', tag='a')
        def one(x: int) -> int:
            return x

        @cachu.cache(ttl=222, backend='file', tag='b')
        def two(x: int) -> int:
            return x

        memory_regions = manager.get_regions(package, ['memory'])
        assert (package, 'memory', 111) in memory_regions
        assert (package, 'file', 222) not in memory_regions

        ttl_regions = manager.get_regions(package, None, 222)
        assert ttl_regions == {(package, 'file', 222)}

    def test_materialize_is_idempotent(self):
        """Materializing twice does not replace a live backend instance.

        Mutation: overwrite self.backends[key] unconditionally, discarding a
        warm backend and its entries mid-process.
        Oracle: object identity of the first instance.
        """
        package = cachu.config._get_caller_package()

        @cachu.cache(ttl=333, backend='memory', tag='idem')
        def one(x: int) -> int:
            return x

        assert manager.materialize(package, ['memory'], 333) == 1
        first = manager.get_backend(package, 'memory', 333)
        first.set('sentinel', 'kept', 300)

        assert manager.materialize(package, ['memory'], 333) == 1
        assert manager.get_backend(package, 'memory', 333) is first
        assert first.get('sentinel') == 'kept'


class TestNothingToClearIsVisible:
    """"No region matched" is reported rather than looking like "no entries".
    """

    def test_missing_region_logs_a_warning(self, caplog):
        """Clearing a package with no declared region warns.

        Mutation: drop the warning, restoring the ambiguity between "cleared
        nothing" and "found nothing to clear".
        Oracle: the documented phrase 'no cache region' in the log record.
        """
        with caplog.at_level(logging.WARNING, logger='cachu.operations'):
            assert cachu.cache_clear(package='package-that-does-not-exist') == 0

        assert any('no cache region' in record.message for record in caplog.records)

    def test_matching_region_with_no_entries_does_not_warn(self, caplog):
        """An empty but real region returns 0 quietly.

        Mutation: warn whenever the count is 0, drowning real clears in noise.
        Oracle: an empty warning list for a declared, materialized region.
        """
        package = cachu.config._get_caller_package()

        @cachu.cache(ttl=444, backend='memory', tag='empty')
        def one(x: int) -> int:
            return x

        with caplog.at_level(logging.WARNING, logger='cachu.operations'):
            assert cachu.cache_clear(tag='empty', package=package) == 0

        assert [r for r in caplog.records if 'no cache region' in r.message] == []


class TestExistingBehaviourIsPreserved:
    """The warm-process paths behave exactly as before.
    """

    def test_explicit_backend_and_ttl_still_clears(self):
        """cache_clear(backend=..., ttl=...) keeps its direct path.

        Mutation: route the explicit form through materialization and lose
        the guarantee that the exact region is built.
        Oracle: hand-counted entry count, 1.
        """
        @cachu.cache(ttl=300, backend='memory', tag='warm')
        def fetch(x: int) -> int:
            return x

        fetch(1)

        assert cachu.cache_clear(tag='warm', backend='memory', ttl=300) == 1

    def test_clear_errors_propagate_regardless_of_fail_open(self, monkeypatch):
        """A clear failure is never swallowed, even under the fail_open default.

        Mutation: wrap backend clears in a blanket try/except, or gate them on
        fail_open. Clearing exists for its side effect, so a silently failed
        clear is worse than a loud one - and it is what let a stale entry be
        served in the reported incident.
        Oracle: the sentinel error type raised by the stubbed clear.
        """
        @cachu.cache(ttl=300, backend='memory', tag='warm')
        def fetch(x: int) -> int:
            return x

        fetch(1)

        def boom(self, pattern=None):
            raise RuntimeError('clear exploded')

        monkeypatch.setattr(cachu.backends.MemoryBackend, 'clear', boom)

        assert cachu.get_config().fail_open is True
        with pytest.raises(RuntimeError, match='clear exploded'):
            cachu.cache_clear(tag='warm')

    def test_cold_clear_errors_propagate_too(self, monkeypatch):
        """A materialized region's clear failure is not swallowed either.

        Mutation: swallow errors for regions this call materialized, which
        makes the same cache_clear raise or warn depending only on whether
        the process happened to be warm.
        Oracle: the sentinel error type, raised from a cold process.
        """
        @cachu.cache(ttl=300, backend='memory', tag='cold')
        def fetch(x: int) -> int:
            return x

        def boom(self, pattern=None):
            raise RuntimeError('clear exploded')

        monkeypatch.setattr(cachu.backends.MemoryBackend, 'clear', boom)

        with pytest.raises(RuntimeError, match='clear exploded'):
            cachu.cache_clear(tag='cold')
