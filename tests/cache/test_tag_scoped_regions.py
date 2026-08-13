"""Tests that a tag-scoped clear visits only the regions declaring that tag.

Notes
-----
- The region registry recorded (package, backend, ttl) but not the tag, so
  `cache_clear(tag=...)` could only pass the tag on as a KEY GLOB and had to
  visit every backend the package had declared.
- A tag pinned to `backend='memory'` therefore constructed and dialed Redis
  purely because `configure(redis_url=...)` had been called for unrelated
  caches: 42.21s per call against a blackholed endpoint, around 27 of one
  test module's 29.7 minutes.
- Recording the tag at decoration time makes the region set the narrowing
  unit, so a memory-pinned tag does no network I/O at all.
"""
import logging

import cachu
import pytest
from cachu.backends.redis import RedisBackend
from cachu.config import _get_caller_package
from cachu.manager import CacheManager, manager

UNREACHABLE_URL = 'redis://127.0.0.1:6399/0'


@pytest.fixture(autouse=True)
def unreachable_redis():
    """Point the package's Redis at a closed port, so any dial is a failure.

    A refused connection is fast, so the assertions stay about WHETHER Redis
    was touched rather than about how long touching it took.
    """
    cachu.configure(
        redis_url=UNREACHABLE_URL,
        redis_socket_timeout=0.05,
        redis_retry_count=0,
        package=_get_caller_package(),
    )


@pytest.fixture
def redis_clears(monkeypatch):
    """Record every glob handed to RedisBackend.clear without raising.

    A sweeping clear swallows and logs per-region failures, so a spy that
    raised would be absorbed and the test would pass while the fan-out it
    is meant to catch still happened.
    """
    seen = []

    def spy(self, pattern=None):
        seen.append(pattern)
        return 0

    monkeypatch.setattr(RedisBackend, 'clear', spy)
    return seen


@pytest.fixture
def built_backends(monkeypatch):
    """Record the backend types the manager constructs during a test.
    """
    seen = []
    original = CacheManager._create_backend

    def recording(self, package, backend_type, ttl):
        seen.append(backend_type)
        return original(self, package, backend_type, ttl)

    monkeypatch.setattr(CacheManager, '_create_backend', recording)
    return seen


class TestRegionsRecordTheirTags:
    """The registry knows which tags each region declared.
    """

    def test_get_regions_narrows_on_the_declared_tag(self):
        """A tag filter keeps only the regions whose decorator declared it.

        Mutation: register the region without its tag, restoring the 0.4.0
        registry that could not narrow at all.
        Oracle: hand-written region keys for two decorators that differ only
        in backend and tag.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='memory', tag='auth')
        def auth(user: str) -> str:
            return user

        @cachu.cache(ttl=300, backend='redis', tag='providers')
        def providers(name: str) -> str:
            return name

        assert manager.get_regions(package, tag='auth') == {(package, 'memory', 300)}
        assert manager.get_regions(package, tag='providers') == {(package, 'redis', 300)}
        assert manager.get_regions(package) == {
            (package, 'memory', 300), (package, 'redis', 300)}

    def test_a_region_shared_by_two_tags_matches_either(self):
        """Two decorators on one region contribute both tags.

        Mutation: overwrite the region's tag on each registration instead of
        adding to it, so the first-declared tag can no longer be cleared.
        Oracle: the same region key returned for both tags.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='memory', tag='auth')
        def auth(user: str) -> str:
            return user

        @cachu.cache(ttl=300, backend='memory', tag='docs')
        def docs(name: str) -> str:
            return name

        assert manager.get_regions(package, tag='auth') == {(package, 'memory', 300)}
        assert manager.get_regions(package, tag='docs') == {(package, 'memory', 300)}

    def test_an_untagged_decorator_declares_no_tag(self):
        """The empty tag is not a tag, so it never matches a tag filter.

        Mutation: record '' as a declared tag, which makes
        `cache_clear(tag='')` read as "the untagged caches" while the key
        glob it builds matches everything.
        Oracle: the empty region set for a filter no decorator declared.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='memory')
        def plain(x: int) -> int:
            return x

        assert manager.get_regions(package) == {(package, 'memory', 300)}
        assert manager.get_regions(package, tag='anything') == set()


class TestTagClearDoesNotFanOut:
    """A memory-pinned tag performs no Redis work.
    """

    def test_memory_pinned_tag_clear_never_touches_redis(self, redis_clears, built_backends):
        """The reported 42s clear: Redis is neither built nor dialed.

        Mutation: drop the tag filter from the region lookup, restoring the
        fan-out that visited every declared backend of the package.
        Oracle: the recorded RedisBackend.clear globs (none) and constructed
        backend types (no 'redis'), plus the entry count the clear reports.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='memory', tag='auth')
        def auth(user: str) -> str:
            return user

        @cachu.cache(ttl=300, backend='redis', tag='providers')
        def providers(name: str) -> str:
            return name

        auth('ana')

        assert cachu.cache_clear(tag='auth') == 1
        assert redis_clears == []
        assert 'redis' not in built_backends
        assert (package, 'redis', 300) not in manager.backends

    async def test_async_memory_pinned_tag_clear_never_touches_redis(
            self, redis_clears, built_backends):
        """The async clear narrows identically.

        Mutation: narrow only the sync path, leaving an async service paying
        the full socket budget on every fixture teardown.
        Oracle: the recorded globs and constructed backend types.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='memory', tag='auth')
        async def auth(user: str) -> str:
            return user

        @cachu.cache(ttl=300, backend='redis', tag='providers')
        async def providers(name: str) -> str:
            return name

        await auth('ana')

        assert await cachu.async_cache_clear(tag='auth') == 1
        assert redis_clears == []
        assert (package, 'redis', 300) not in manager.backends

    def test_global_clear_does_not_reopen_the_fan_out(self, redis_clears):
        """global_clear crosses key prefixes, not tags.

        Mutation: skip the tag narrowing whenever global_clear is set, which
        is exactly the call the reported consumer makes
        (`cache_clear(tag='auth', global_clear=True, package='finx')`).
        Oracle: the recorded RedisBackend.clear globs, none.
        """
        @cachu.cache(ttl=300, backend='memory', tag='auth')
        def auth(user: str) -> str:
            return user

        @cachu.cache(ttl=300, backend='redis', tag='providers')
        def providers(name: str) -> str:
            return name

        auth('ana')

        assert cachu.cache_clear(tag='auth', global_clear=True) == 1
        assert redis_clears == []

    def test_a_redis_pinned_tag_clear_still_visits_redis(self, redis_clears):
        """Narrowing must not turn a Redis-tagged clear into a no-op.

        Mutation: narrow to the default backend only, or drop Redis regions
        from the sweep, which would silently stop clearing the entries a
        caller asked about.
        Oracle: the recorded glob, hand-written for the 300s region and the
        'providers' tag.
        """
        @cachu.cache(ttl=300, backend='memory', tag='auth')
        def auth(user: str) -> str:
            return user

        @cachu.cache(ttl=300, backend='redis', tag='providers')
        def providers(name: str) -> str:
            return name

        cachu.cache_clear(tag='providers')

        assert redis_clears == ['5m:test:*|providers|*']

    def test_an_untagged_clear_still_sweeps_every_region(self, redis_clears):
        """Without a tag the sweep is unchanged.

        Mutation: narrow by tag even when no tag was given, which would make
        `cache_clear()` miss every region and quietly clear nothing.
        Oracle: the recorded glob for the Redis region, which must be
        visited by an untagged sweep.
        """
        @cachu.cache(ttl=300, backend='memory', tag='auth')
        def auth(user: str) -> str:
            return user

        @cachu.cache(ttl=300, backend='redis', tag='providers')
        def providers(name: str) -> str:
            return name

        auth('ana')

        assert cachu.cache_clear() == 1
        assert redis_clears == ['5m:test:*|*']

    def test_an_already_live_redis_region_is_skipped_too(self, redis_clears):
        """The saving must hold in a WARM process, not only a cold one.

        Mutation: apply the tag filter at materialization only, leaving
        `iter_backends`/`_matching` unfiltered. A region built earlier in the
        process is live regardless of which tag asked for it, so a
        long-running service - the case that actually pays this cost, once
        per fixture teardown or admin call - would keep dialing Redis while
        a cold test process looked fixed.
        Oracle: the recorded RedisBackend.clear globs, none, with the Redis
        region provably live before the clear.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='memory', tag='auth')
        def auth(user: str) -> str:
            return user

        @cachu.cache(ttl=300, backend='redis', tag='providers')
        def providers(name: str) -> str:
            return name

        auth('ana')
        manager.get_backend(package, 'redis', 300)
        assert (package, 'redis', 300) in manager.backends

        assert cachu.cache_clear(tag='auth') == 1
        assert redis_clears == []

    def test_a_tag_that_normalizes_to_another_form_still_matches(self):
        """A tag is looked up in the form the KEY carries, not as written.

        Mutation: record the raw tag while the key stores the normalized
        one. `@cache(tag='a|b')` writes keys under the tag 'a.b', so
        `cache_clear(tag='a.b')` builds a glob that matches them yet finds
        no region declaring it - the clear silently does nothing while its
        own glob was correct.
        Oracle: hand-counted invocation count, 2 - the entry must recompute.
        """
        calls = []

        @cachu.cache(ttl=300, backend='memory', tag='a|b')
        def fetch(x: int) -> int:
            calls.append(x)
            return x

        fetch(1)

        assert cachu.cache_clear(tag='a.b') == 1

        fetch(1)
        assert len(calls) == 2

    def test_a_tag_clear_still_narrows_within_one_region(self):
        """Two tags in one region are still separated by the key glob.

        Mutation: rely on the region filter alone and stop scoping the glob
        by tag, which would clear a co-located tag's entries as well.
        Oracle: hand-counted invocation counts, one recompute for the
        cleared tag and none for its neighbor.
        """
        auth_calls = []
        docs_calls = []

        @cachu.cache(ttl=300, backend='memory', tag='auth')
        def auth(user: str) -> str:
            auth_calls.append(user)
            return user

        @cachu.cache(ttl=300, backend='memory', tag='docs')
        def docs(name: str) -> str:
            docs_calls.append(name)
            return name

        auth('ana')
        docs('ana')

        assert cachu.cache_clear(tag='auth') == 1

        auth('ana')
        docs('ana')

        assert len(auth_calls) == 2
        assert len(docs_calls) == 1


class TestUndeclaredTagIsVisible:
    """Narrowing must not clear nothing silently.
    """

    def test_clearing_a_tag_no_region_declared_names_the_declared_tags(self, caplog):
        """A typo, or a tag whose module was never imported, is reported.

        Mutation: drop the warning. Narrowing by tag makes "no region
        declares this tag" reachable in a way the 0.4.0 sweep never was: an
        admin process that clears a tag it never imported now clears
        nothing, and would do so silently.
        Oracle: the documented phrase 'no cache region' plus the declared
        tag name in the log record.
        """
        @cachu.cache(ttl=300, backend='memory', tag='auth')
        def auth(user: str) -> str:
            return user

        auth('ana')

        with caplog.at_level(logging.WARNING, logger='cachu.operations'):
            assert cachu.cache_clear(tag='typo') == 0

        messages = [r.message for r in caplog.records]
        assert any('no cache region' in message for message in messages)
        assert any("'auth'" in message for message in messages)

    def test_a_declared_tag_excluded_by_a_filter_is_not_blamed_on_imports(self, caplog):
        """A declared tag filtered out by backend=/ttl= says so.

        Mutation: emit the import hint whenever a tag matched nothing. The
        reader is then sent after a missing import when the real cause is
        the `backend=` they passed, which is the one thing the message was
        supposed to disambiguate.
        Oracle: the log record must name the tag as declared and must NOT
        carry the import wording.
        """
        @cachu.cache(ttl=300, backend='memory', tag='auth')
        def auth(user: str) -> str:
            return user

        auth('ana')

        with caplog.at_level(logging.WARNING, logger='cachu.operations'):
            assert cachu.cache_clear(tag='auth', backend='file') == 0

        messages = [r.message for r in caplog.records if 'no cache region' in r.message]
        assert messages
        assert any('IS declared' in message for message in messages)
        assert not any('never imported' in message for message in messages)

    async def test_async_clearing_an_undeclared_tag_also_warns(self, caplog):
        """The async clear reports it too.

        Mutation: warn on the sync path only.
        Oracle: the documented phrase 'no cache region' in the log record.
        """
        @cachu.cache(ttl=300, backend='memory', tag='auth')
        async def auth(user: str) -> str:
            return user

        await auth('ana')

        with caplog.at_level(logging.WARNING, logger='cachu.operations'):
            assert await cachu.async_cache_clear(tag='typo') == 0

        assert any('no cache region' in r.message for r in caplog.records)
