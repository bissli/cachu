"""Tests that a clear can only ever touch keys of cachu's own shape.

Notes
-----
- `_clear_targets` scoped the clear glob by `key_prefix` only when one was
  configured, and `key_prefix` defaults to ''. A default-configured package
  therefore handed `RedisBackend.clear` a None pattern, which substitutes
  '*' and UNLINKs every key in the logical DB - including keys cachu never
  wrote.
- The reported blast radius was one unauthenticated `POST /cache/clear`
  wiping a service's entire Redis DB, cachu-owned or not.
- The mangled key shape `<region>:<key_prefix><fn_name>|<params>` is a
  namespace cachu already owns, so every clear can be scoped to it without
  a new configuration knob. `global_clear=True` widens the prefix, never
  the namespace.
"""
import fnmatch
import logging
import sqlite3

import cachu
import pytest
import redis as redis_lib
from cachu.backends.memory import MemoryBackend
from cachu.config import _get_caller_package, _registry
from cachu.manager import manager
from cachu.util import make_clear_pattern, make_partial_pattern, mangle_key

FOREIGN_KEYS = (
    'session:abc123',
    'celery-task-meta-9f2c',
    'ratelimit:203.0.113.7',
    'flask_cache_view',
    '5m:no-pipe-here',
)


def _glob_matches(key: str, pattern: str) -> bool:
    """Match with SQLite GLOB, the matcher the file backend clears through.
    """
    with sqlite3.connect(':memory:') as conn:
        return conn.execute('select ? glob ?', (key, pattern)).fetchone()[0] == 1


class TestClearPatternShape:
    """The glob handed to a backend is derived, never absent.
    """

    def test_pattern_is_region_scoped_even_without_a_key_prefix(self):
        """An empty key_prefix still yields a scoped glob.

        Mutation: scope by prefix only when one is configured (the 0.4.0
        behaviour), which returns None here and lets Redis widen it to '*'.
        Oracle: hand-written globs for the 300s region.
        """
        assert make_clear_pattern(None, '', 300) == '5m:*|*'
        assert make_clear_pattern(None, 'test:', 300) == '5m:test:*|*'
        assert make_clear_pattern('users', '', 300) == '5m:*|users|*'
        assert make_clear_pattern('users', 'test:', 300) == '5m:test:*|users|*'

    def test_global_clear_widens_the_prefix_not_the_namespace(self):
        """global_clear drops the key_prefix and keeps the region and shape.

        Mutation: return '*' for a global clear, which is exactly the
        whole-keyspace sweep this bug is about.
        Oracle: hand-written globs; both keep the region segment and the
        `fn_name|params` separator.
        """
        assert make_clear_pattern(None, 'test:', 300, global_clear=True) == '5m:*|*'
        assert make_clear_pattern('users', 'test:', 300, global_clear=True) == '5m:*|users|*'

    def test_pattern_tracks_the_region_of_the_ttl_being_cleared(self):
        """Each TTL region gets its own glob, not one shared '*:' wildcard.

        Mutation: build the glob once from '*:' for every region, which lets
        a clear of the 5m region delete 1h entries of the same function.
        Oracle: `_seconds_to_region_name` naming, hand-applied.
        """
        assert make_clear_pattern(None, '', 30) == '30s:*|*'
        assert make_clear_pattern(None, '', 3600) == '1h:*|*'
        assert make_clear_pattern(None, '', 86400) == '1d:*|*'
        assert make_clear_pattern(None, '', -1) == 'dynamic:*|*'

    def test_pattern_matches_cachu_keys_and_rejects_foreign_ones(self):
        """The glob is tight enough to exclude keys cachu never wrote.

        Mutation: drop the trailing `|` shape pin, so '5m:no-pipe-here' and
        any other key that merely opens with a region-like segment matches.
        Oracle: fnmatch, the matcher the memory backend itself uses, run
        against hand-written cachu-shaped and foreign keys.
        """
        pattern = make_clear_pattern(None, '', 300)

        assert fnmatch.fnmatch(mangle_key('get_user||user_id=1', '', 300), pattern)
        assert fnmatch.fnmatch(mangle_key('get_user||users||user_id=1', '', 300), pattern)

        for foreign in FOREIGN_KEYS:
            assert not fnmatch.fnmatch(foreign, pattern), foreign

    def test_a_key_prefix_holding_a_character_class_still_matches_its_own_keys(self):
        """A key_prefix with '[' must not become a glob character class.

        Mutation: interpolate key_prefix raw. `key_prefix='app[dev]:'` then
        reads as the class `[dev]`, so the clear matches NOTHING and every
        stale entry survives - a silent no-op, which is the worst failure a
        clear has.
        Oracle: fnmatch and SQLite GLOB, the matchers the memory and file
        backends use, against the key `mangle_key` produces for that prefix.
        """
        prefix = 'app[dev]:'
        key = mangle_key('fetch||x=1', prefix, 300)
        pattern = make_clear_pattern(None, prefix, 300)

        assert fnmatch.fnmatchcase(key, pattern)
        assert _glob_matches(key, pattern)

    def test_a_key_prefix_holding_a_star_cannot_reach_another_prefix(self):
        """A key_prefix with '*' must not widen past itself.

        Mutation: interpolate key_prefix raw. `key_prefix='p*x:'` then also
        matches 'prod-x:', so a clear scoped to one prefix deletes another
        prefix's entries - the same "reaches keys it does not own" fault as
        an unscoped '*', reached through the knob that is supposed to
        prevent it.
        Oracle: fnmatch against a key written under a different prefix that
        the raw glob would swallow.
        """
        pattern = make_clear_pattern(None, 'p*x:', 300)

        assert fnmatch.fnmatchcase(mangle_key('fetch||x=1', 'p*x:', 300), pattern)
        assert not fnmatch.fnmatchcase(
            mangle_key('fetch||x=1', 'prod-x:', 300), pattern)

    def test_a_tag_holding_a_star_cannot_reach_another_tag(self):
        """A tag with '*' matches itself, not every tag sharing its stem.

        Mutation: interpolate the normalized tag raw, so
        `cache_clear(tag='user*')` also clears the 'userSECRET' tag.
        Oracle: fnmatch against both tags' keys.
        """
        pattern = make_clear_pattern('user*', '', 300)

        assert fnmatch.fnmatchcase(mangle_key('fetch||user*||x=1', '', 300), pattern)
        assert not fnmatch.fnmatchcase(
            mangle_key('fetch||userSECRET||x=1', '', 300), pattern)

    def test_escaping_is_identical_under_fnmatch_and_sqlite_glob(self):
        """The escape form has to hold for every backend's matcher.

        Mutation: escape with a backslash, which Redis honours but fnmatch
        and SQLite GLOB treat as a literal character - so the memory and
        file backends would silently stop matching.
        Oracle: differential agreement between fnmatch and SQLite GLOB on
        every metacharacter, plus a non-match control.
        """
        for prefix in ('p[0]:', 'a?b:', 'p*x:', 'plain:'):
            key = mangle_key('fetch||x=1', prefix, 300)
            pattern = make_clear_pattern(None, prefix, 300)
            assert fnmatch.fnmatchcase(key, pattern) is True, prefix
            assert _glob_matches(key, pattern) is True, prefix

            other = mangle_key('fetch||x=1', 'unrelated:', 300)
            assert fnmatch.fnmatchcase(other, pattern) is False, prefix
            assert _glob_matches(other, pattern) is False, prefix

    def test_a_metacharacter_prefix_clears_end_to_end(self):
        """The escaped glob really clears through cache_clear.

        Mutation: escape the pattern but not consistently with the key, so
        the unit assertions above pass while the real clear removes nothing.
        Oracle: hand-counted invocation count, 2 across a call, a clear and
        a second call.
        """
        _registry._default.key_prefix = 'app[dev]:'
        calls = []

        @cachu.cache(ttl=300, backend='memory')
        def fetch(x: int) -> int:
            calls.append(x)
            return x

        fetch(1)
        fetch(1)
        assert len(calls) == 1

        assert cachu.cache_clear() == 1

        fetch(1)
        assert len(calls) == 2

    def test_a_float_ttl_lands_in_the_same_region_as_its_int(self):
        """300.0 and 300 name one region, because they ARE one region.

        Mutation: drop the int() normalisation from
        `_seconds_to_region_name`. `manager` keys regions by a tuple and
        300 == 300.0 hashes equal, so the two decorators share one region
        while writing keys under '5m' and '5.0m' - and every clear builds
        one of the two names and can never match the other's entries. A
        `timedelta(minutes=5).total_seconds()` or a float from JSON config
        is all it takes, and which function is stranded depends on import
        order.
        Oracle: hand-written region name, plus fnmatch of the float-written
        key against the int-built glob.
        """
        assert make_clear_pattern(None, '', 300.0) == make_clear_pattern(None, '', 300)
        assert fnmatch.fnmatchcase(
            mangle_key('fetch||x=1', '', 300.0), make_clear_pattern(None, '', 300))

    def test_a_float_ttl_region_is_cleared_end_to_end(self):
        """Two decorators differing only in ttl literal are cleared together.

        Mutation: the same one. Without it this clear reports 1 instead of 2
        and one function keeps serving pre-clear values for the region's
        whole life.
        Oracle: hand-counted invocation counts, 2 each - both must recompute.
        """
        int_calls = []
        float_calls = []

        @cachu.cache(ttl=300, backend='memory')
        def by_int(x: int) -> int:
            int_calls.append(x)
            return x

        @cachu.cache(ttl=300.0, backend='memory')
        def by_float(x: int) -> int:
            float_calls.append(x)
            return x

        by_int(1)
        by_float(1)

        assert cachu.cache_clear() == 2

        by_int(1)
        by_float(1)

        assert len(int_calls) == 2
        assert len(float_calls) == 2

    def test_pattern_excludes_the_dogpile_lock_of_its_own_keys(self):
        """A clear must not delete a lock a live caller is holding.

        Mutation: fall through to '*', which UNLINKs 'lock:<cache_key>' and
        leaves two callers inside one critical section.
        Oracle: fnmatch against the lock key `get_mutex` builds.
        """
        pattern = make_clear_pattern(None, '', 300)
        cache_key = mangle_key('get_user||user_id=1', '', 300)

        assert not fnmatch.fnmatch(f'lock:{cache_key}', pattern)


class TestClearNeverWidensAtTheOperationsBoundary:
    """cache_clear itself never hands a backend None or '*'.
    """

    def test_default_configured_clear_passes_a_scoped_glob(self, monkeypatch):
        """A clear with no tag and no key_prefix is still scoped.

        Mutation: restore the `if cfg.key_prefix:` guard in _clear_targets,
        so the backend receives None and Redis substitutes '*'.
        Oracle: the captured pattern list, compared to the hand-written glob
        for the one declared region.
        """
        _registry._default.key_prefix = ''
        seen = []
        original = MemoryBackend.clear

        def spy(self, pattern=None):
            seen.append(pattern)
            return original(self, pattern)

        monkeypatch.setattr(MemoryBackend, 'clear', spy)

        @cachu.cache(ttl=300, backend='memory')
        def fetch(x: int) -> int:
            return x

        fetch(1)
        assert cachu.cache_clear() == 1
        assert seen == ['5m:*|*']

    async def test_async_default_configured_clear_passes_a_scoped_glob(self, monkeypatch):
        """The async clear is scoped identically.

        Mutation: fix only the sync path, leaving async callers able to wipe
        a shared Redis DB.
        Oracle: the captured pattern list for the one declared region.
        """
        _registry._default.key_prefix = ''
        seen = []
        original = MemoryBackend.aclear

        async def spy(self, pattern=None):
            seen.append(pattern)
            return await original(self, pattern)

        monkeypatch.setattr(MemoryBackend, 'aclear', spy)

        @cachu.cache(ttl=300, backend='memory')
        async def fetch(x: int) -> int:
            return x

        await fetch(1)
        assert await cachu.async_cache_clear() == 1
        assert seen == ['5m:*|*']

    def test_explicit_backend_and_ttl_is_scoped_too(self, monkeypatch):
        """The direct (backend, ttl) path shares the scoping.

        Mutation: scope only the region-sweeping path, leaving the form the
        reported consumer's `POST /cache/clear` route actually calls
        unscoped.
        Oracle: the captured pattern list.
        """
        _registry._default.key_prefix = ''
        seen = []
        original = MemoryBackend.clear

        def spy(self, pattern=None):
            seen.append(pattern)
            return original(self, pattern)

        monkeypatch.setattr(MemoryBackend, 'clear', spy)

        @cachu.cache(ttl=300, backend='memory')
        def fetch(x: int) -> int:
            return x

        fetch(1)
        assert cachu.cache_clear(backend='memory', ttl=300) == 1
        assert seen == ['5m:*|*']

    def test_a_prefixless_clear_still_removes_its_own_entries(self):
        """Scoping must not turn the clear into a no-op.

        Mutation: scope to a glob that matches nothing, which would make
        every clear silently succeed - the failure mode this library
        already had once.
        Oracle: hand-counted invocation count, 2 across a call, a clear and
        a second call.
        """
        _registry._default.key_prefix = ''
        calls = []

        @cachu.cache(ttl=300, backend='memory')
        def fetch(x: int) -> int:
            calls.append(x)
            return x

        fetch(1)
        fetch(1)
        assert len(calls) == 1

        assert cachu.cache_clear() == 1

        fetch(1)
        assert len(calls) == 2


class TestHelperClearIsScopedToo:
    """`fn.clear(_global=True)` obeys the same namespace invariant.
    """

    def test_a_global_helper_clear_is_anchored_to_its_region(self):
        """The glob keeps its region segment when the prefix is dropped.

        Mutation: build the global glob as `*<fn>|*`, unanchored at the
        front. It then matches a foreign `worker:fetch|job-7`, the same
        function's entries in other TTL regions, and `lock:<key>` - so
        `.clear(_global=True)` could release a mutex a live caller holds.
        `_global` is documented as skipping key_prefix scoping and nothing
        more.
        Oracle: fnmatch against one key of each shape.
        """
        pattern = make_partial_pattern('fetch', '', 'pfx:', 300, global_clear=True)

        assert fnmatch.fnmatchcase(mangle_key('fetch||x=1', 'pfx:', 300), pattern)
        assert fnmatch.fnmatchcase(mangle_key('fetch||x=1', 'other:', 300), pattern)
        assert not fnmatch.fnmatchcase(mangle_key('fetch||x=9', 'pfx:', 3600), pattern)
        assert not fnmatch.fnmatchcase('worker:fetch|job-7', pattern)
        assert not fnmatch.fnmatchcase(
            f'lock:{mangle_key("fetch||x=1", "pfx:", 300)}', pattern)

    def test_a_global_helper_clear_still_crosses_key_prefixes(self):
        """Anchoring must not cost `_global` its documented purpose.

        Mutation: scope the global glob by key_prefix after all, which makes
        `_global=True` identical to a plain clear and silently strands
        entries written under a previous prefix.
        Oracle: hand-counted entries cleared, 2, one per prefix.
        """
        package = _get_caller_package()
        backend = manager.get_backend(package, 'memory', 300)

        @cachu.cache(ttl=300, backend='memory')
        def fetch(x: int) -> int:
            return x

        backend.set(mangle_key('fetch||x=1', 'dev:', 300), 'dev', 300)
        backend.set(mangle_key('fetch||x=1', 'prod:', 300), 'prod', 300)

        _registry._default.key_prefix = 'dev:'
        assert fetch.clear(_global=True) == 2

    def test_partial_pattern_escapes_its_prefix_and_tag(self):
        """`.clear()` escapes metacharacters exactly as cache_clear does.

        Mutation: escape in `make_clear_pattern` only. `fn.clear()` under
        `key_prefix='app[dev]:'` then matches nothing, so the helper method
        silently stops clearing while the module-level function works.
        Oracle: fnmatch of the key that prefix produces.
        """
        key = mangle_key('fetch||users||x=1', 'app[dev]:', 300)
        pattern = make_partial_pattern('fetch', 'users', 'app[dev]:', 300)

        assert fnmatch.fnmatchcase(key, pattern)
        assert _glob_matches(key, pattern)


@pytest.mark.redis
class TestClearLeavesForeignRedisKeysAlone:
    """The reported production incident, against a real Redis.
    """

    @pytest.fixture
    def client(self, redis_docker):
        """An independent Redis client, used as the oracle for what survives.
        """
        from _fixtures.redis import redis_test_config

        conn = redis_lib.Redis(
            host=redis_test_config.host, port=redis_test_config.port, db=0)
        yield conn
        conn.close()

    def test_a_default_configured_clear_keeps_non_cachu_keys(self, client):
        """One cache_clear() cannot empty the logical DB.

        Mutation: restore the pattern fall-through to '*'. `scan_iter`
        then matches every key in the DB and the pipeline UNLINKs all of
        them, which is the unauthenticated whole-DB wipe in the report.
        Oracle: an independent client reading each foreign key back, plus
        the cachu entry's own invocation count.
        """
        _registry._default.key_prefix = ''
        calls = []

        for key in FOREIGN_KEYS:
            client.set(key, b'not-cachus')

        @cachu.cache(ttl=300, backend='redis')
        def fetch(x: int) -> int:
            calls.append(x)
            return x * 2

        assert fetch(21) == 42
        assert fetch(21) == 42
        assert len(calls) == 1

        assert cachu.cache_clear() == 1

        for key in FOREIGN_KEYS:
            assert client.get(key) == b'not-cachus', key

        assert fetch(21) == 42
        assert len(calls) == 2

    def test_a_global_clear_keeps_non_cachu_keys(self, client):
        """global_clear=True crosses key prefixes, not the namespace.

        Mutation: treat global_clear as permission to pass '*'. The knob
        exists to reach another key_prefix's entries, not another
        library's keys.
        Oracle: an independent client reading the foreign keys back while
        both cachu prefixes are gone.
        """
        package = _get_caller_package()
        backend = manager.get_backend(package, 'redis', 300)

        for key in FOREIGN_KEYS:
            client.set(key, b'not-cachus')

        dev_key = mangle_key('fetch||x=1', 'dev:', 300)
        prod_key = mangle_key('fetch||x=1', 'prod:', 300)
        backend.set(dev_key, 'dev', 300)
        backend.set(prod_key, 'prod', 300)

        _registry._default.key_prefix = 'dev:'
        assert cachu.cache_clear(backend='redis', ttl=300, global_clear=True) == 2

        for key in FOREIGN_KEYS:
            assert client.get(key) == b'not-cachus', key

    def test_a_live_dogpile_lock_survives_a_clear(self, client):
        """Clearing entries must not release another caller's lock.

        Mutation: fall through to '*'. The lock key is UNLINKed, so the
        waiter that was about to observe a held lock takes it too and both
        callers run the critical section.
        Oracle: an independent client reading the lock key back.
        """
        _registry._default.key_prefix = ''
        package = _get_caller_package()
        backend = manager.get_backend(package, 'redis', 300)

        @cachu.cache(ttl=300, backend='redis')
        def fetch(x: int) -> int:
            return x

        fetch(1)

        mutex = backend.get_mutex(mangle_key('fetch||x=2', '', 300))
        assert mutex.acquire(timeout=0) is True
        lock_key = f'lock:{mangle_key("fetch||x=2", "", 300)}'
        assert client.get(lock_key) is not None

        assert cachu.cache_clear() == 1

        assert client.get(lock_key) is not None
        mutex.release()

    def test_a_metacharacter_key_prefix_clears_on_redis_too(self, client):
        """Redis's own glob must honour the same escape form as the others.

        Mutation: escape with a backslash instead of a single-character
        class. Redis accepts it, so this passes while the memory and file
        backends silently stop matching; escape with nothing and Redis reads
        `[dev]` as a class, so nothing is cleared here either.
        Oracle: an independent client reading both keys back - the escaped
        prefix's entry gone, a neighbouring prefix's entry untouched.
        """
        package = _get_caller_package()
        backend = manager.get_backend(package, 'redis', 300)

        mine = mangle_key('fetch||x=1', 'app[dev]:', 300)
        neighbour = mangle_key('fetch||x=1', 'appd:', 300)
        backend.set(mine, 'mine', 300)
        backend.set(neighbour, 'neighbour', 300)

        _registry._default.key_prefix = 'app[dev]:'
        assert cachu.cache_clear(backend='redis', ttl=300) == 1

        assert client.get(mine) is None
        assert client.get(neighbour) is not None

    def test_clear_does_not_scan_the_whole_keyspace(self, client, caplog):
        """A scoped clear reports only its own entries as cleared.

        Mutation: widen the glob to '*'; the returned count then includes
        every foreign key, so the count itself becomes the tell.
        Oracle: hand-counted cachu entries, 3 written and 3 reported, with
        5 foreign keys present throughout.
        """
        _registry._default.key_prefix = ''

        for key in FOREIGN_KEYS:
            client.set(key, b'not-cachus')

        @cachu.cache(ttl=300, backend='redis')
        def fetch(x: int) -> int:
            return x

        for value in (1, 2, 3):
            fetch(value)

        with caplog.at_level(logging.WARNING, logger='cachu.operations'):
            assert cachu.cache_clear() == 3

        assert [r for r in caplog.records if 'no cache region' in r.message] == []
        assert client.dbsize() >= len(FOREIGN_KEYS)


@pytest.mark.redis
def test_clearing_one_ttl_region_leaves_another_alone(redis_docker):
    """Region scoping is what keeps a 5m clear off the 1h entries.

    Mutation: build the glob from '*:' rather than the region being
    cleared, so `cache_clear(ttl=300)` also deletes the 3600s entries of
    the same function name.
    Oracle: hand-counted invocation counts, one recompute for the cleared
    region and none for the untouched one.
    """
    short_calls = []
    long_calls = []

    @cachu.cache(ttl=300, backend='redis')
    def fetch_short(x: int) -> int:
        short_calls.append(x)
        return x

    @cachu.cache(ttl=3600, backend='redis')
    def fetch_long(x: int) -> int:
        long_calls.append(x)
        return x

    fetch_short(1)
    fetch_long(1)

    assert cachu.cache_clear(ttl=300) == 1

    fetch_short(1)
    fetch_long(1)

    assert len(short_calls) == 2
    assert len(long_calls) == 1
