"""Tests for scoping disable()/enable() to a package or a tag.

Notes
-----
- cachu.disable() took no arguments and switched off every cache in the
  process.
- A service with one optional cache and one load-bearing cache could not
  turn off the first without silently turning off the second: the reported
  incident disabled a document cache whose directory was unwritable and,
  without anyone noticing, put a database read on every authorization
  request.
"""
import cachu
import pytest


@pytest.fixture(autouse=True)
def restore_scopes():
    """Leave the process fully enabled whatever a test switched off.
    """
    yield
    cachu.enable()


def _counting_cache(**decorator_kwargs):
    """Build a decorated function plus the list recording its invocations.
    """
    calls = []

    @cachu.cache(ttl=300, backend='memory', **decorator_kwargs)
    def fetch(key: int) -> int:
        calls.append(key)
        return key * 2

    return fetch, calls


class TestGlobalDisableUnchanged:
    """The no-argument form keeps its process-wide meaning.
    """

    def test_disable_with_no_arguments_stops_every_cache(self):
        """disable() still switches off caches in unrelated packages.

        Mutation: make the no-argument form scope itself to the caller's
        package, silently narrowing an API callers rely on for test setup.
        Oracle: hand-counted invocation counts, 2 each - both caches miss on
        the second call.
        """
        alpha, alpha_calls = _counting_cache(package='alpha', tag='a')
        beta, beta_calls = _counting_cache(package='beta', tag='b')

        alpha(1)
        beta(1)
        cachu.disable()
        alpha(1)
        beta(1)

        assert len(alpha_calls) == 2
        assert len(beta_calls) == 2

    def test_is_disabled_with_no_arguments_reports_the_global_flag(self):
        """is_disabled() keeps its zero-argument contract.

        Mutation: require a scope argument, breaking existing callers.
        Oracle: the global flag as set, True then False.
        """
        assert cachu.is_disabled() is False
        cachu.disable()
        assert cachu.is_disabled() is True
        cachu.enable()
        assert cachu.is_disabled() is False


class TestPackageScope:
    """disable(package=...) leaves other packages caching.
    """

    def test_only_the_named_package_stops_caching(self):
        """The load-bearing sibling cache keeps working.

        Mutation: ignore the package argument and set the global flag.
        Oracle: hand-counted invocation counts - 2 for the disabled package,
        1 for the sibling that must still hit.
        """
        alpha, alpha_calls = _counting_cache(package='alpha', tag='a')
        beta, beta_calls = _counting_cache(package='beta', tag='b')

        alpha(1)
        beta(1)
        cachu.disable(package='alpha')
        alpha(1)
        beta(1)

        assert len(alpha_calls) == 2
        assert len(beta_calls) == 1

    def test_targeted_enable_restores_only_that_package(self):
        """enable(package=...) lifts one scope and leaves the others.

        Mutation: have the targeted enable clear every scope.
        Oracle: hand-counted invocation counts - alpha hits again (1 further
        call total), beta keeps missing.
        """
        alpha, alpha_calls = _counting_cache(package='alpha', tag='a')
        beta, beta_calls = _counting_cache(package='beta', tag='b')

        cachu.disable(package='alpha')
        cachu.disable(package='beta')
        alpha(1)
        beta(1)

        cachu.enable(package='alpha')
        alpha(1)
        alpha(1)
        beta(1)

        assert len(alpha_calls) == 2
        assert len(beta_calls) == 2

    def test_bare_enable_clears_scoped_disables(self):
        """enable() with no arguments restores a fully enabled process.

        Mutation: clear only the global flag, stranding scoped disables that
        no documented call can lift.
        Oracle: hand-counted invocation count, 1 - the cache must hit again.
        """
        alpha, alpha_calls = _counting_cache(package='alpha', tag='a')

        cachu.disable(package='alpha')
        cachu.enable()
        alpha(1)
        alpha(1)

        assert len(alpha_calls) == 1

    def test_scoped_enable_cannot_lift_a_global_disable(self):
        """The global switch wins over a targeted enable.

        Mutation: have the scoped enable clear the global flag as a side
        effect, so `enable(package='a')` silently re-enables the whole
        process - the opposite of the scoping this API exists to provide.
        Oracle: hand-counted invocation count, 2 - the cache still misses.
        """
        alpha, alpha_calls = _counting_cache(package='alpha', tag='a')

        cachu.disable()
        cachu.enable(package='alpha')
        alpha(1)
        alpha(1)

        assert len(alpha_calls) == 2
        assert cachu.is_disabled('alpha') is True

        cachu.enable()
        alpha(1)
        alpha(1)

        assert len(alpha_calls) == 3


class TestTagScope:
    """disable(tag=...) switches off one family of caches.
    """

    def test_only_the_named_tag_stops_caching(self):
        """A tagged cache is bypassed while its untagged peer keeps caching.

        Mutation: match the tag against the package set, or ignore tags.
        Oracle: hand-counted invocation counts - 2 for the disabled tag, 1
        for the peer.
        """
        docs, docs_calls = _counting_cache(tag='docs')
        authz, authz_calls = _counting_cache(tag='authz')

        docs(1)
        authz(1)
        cachu.disable(tag='docs')
        docs(1)
        authz(1)

        assert len(docs_calls) == 2
        assert len(authz_calls) == 1

    def test_empty_tag_scope_is_rejected(self):
        """disable(tag='') raises rather than seeding a scope that matches nothing.

        Mutation: accept the empty tag. It is the decorator default, so the
        call reads as "switch off the untagged caches" but the truthiness
        guard in is_disabled makes it a permanent silent no-op.
        Oracle: ValueError, and the untagged cache still hitting afterwards.
        """
        untagged, untagged_calls = _counting_cache()

        untagged(1)

        with pytest.raises(ValueError, match='tag must be a non-empty name'):
            cachu.disable(tag='')
        with pytest.raises(ValueError, match='tag must be a non-empty name'):
            cachu.enable(tag='')
        with pytest.raises(ValueError, match='package must be a non-empty name'):
            cachu.disable(package='')

        untagged(1)

        assert len(untagged_calls) == 1
        assert cachu.get_disabled_scopes().tags == frozenset()

    def test_untagged_caches_are_never_matched_by_a_tag_scope(self):
        """A disabled tag never bypasses caches declared without a tag.

        Mutation: drop the truthiness guard in is_disabled, so the default
        tag='' would match any scope set holding the empty string.
        Oracle: hand-counted invocation count, 1 - the untagged cache hits.
        """
        untagged, untagged_calls = _counting_cache()
        tagged, tagged_calls = _counting_cache(tag='docs')

        untagged(1)
        tagged(1)
        cachu.disable(tag='docs')
        untagged(1)
        tagged(1)

        assert len(untagged_calls) == 1
        assert len(tagged_calls) == 2

    def test_scopes_combine_as_or_not_and(self):
        """Matching either scope is enough to bypass a cache.

        Mutation: require both scopes to match before bypassing.
        Oracle: hand-counted invocation count, 2 - the cache matches on tag
        alone even though its package was never disabled.
        """
        tagged, tagged_calls = _counting_cache(package='alpha', tag='docs')

        tagged(1)
        cachu.disable(tag='docs')
        tagged(1)

        assert len(tagged_calls) == 2


class TestIntrospection:
    """The scoped state is observable.
    """

    def test_is_disabled_answers_for_a_scope(self):
        """is_disabled(package, tag) reports the scoped state.

        Mutation: always answer from the global flag.
        Oracle: the scopes exactly as disabled.
        """
        cachu.disable(package='alpha')
        cachu.disable(tag='docs')

        assert cachu.is_disabled('alpha') is True
        assert cachu.is_disabled('beta') is False
        assert cachu.is_disabled(None, 'docs') is True
        assert cachu.is_disabled(None, 'authz') is False

    def test_get_disabled_scopes_reports_both_sets(self):
        """get_disabled_scopes exposes the flag and both scope sets, immutably.

        Mutation: return the live mutable sets, letting a caller corrupt
        library state. A bare `== frozenset({...})` cannot catch that, since
        set and frozenset compare equal, so the type is asserted too.
        Oracle: the scopes exactly as disabled, plus frozenset identity.
        """
        cachu.disable(package='alpha')
        cachu.disable(tag='docs')

        scopes = cachu.get_disabled_scopes()

        assert scopes.globally is False
        assert type(scopes.packages) is frozenset
        assert type(scopes.tags) is frozenset
        assert scopes.packages == frozenset({'alpha'})
        assert scopes.tags == frozenset({'docs'})

    def test_disabled_scopes_snapshot_does_not_track_later_changes(self):
        """The returned snapshot is a copy, not a live view.

        Mutation: return the module-level sets directly, so a caller holding
        an old snapshot silently sees later disables.
        Oracle: the scope set as it stood when the snapshot was taken.
        """
        cachu.disable(package='alpha')
        snapshot = cachu.get_disabled_scopes()

        cachu.disable(package='beta')

        assert snapshot.packages == frozenset({'alpha'})


class TestAsyncScope:
    """The async wrapper honours the same scopes.
    """

    async def test_async_package_scope(self):
        """An async cache in a disabled package is bypassed.

        Mutation: pass no scope to is_disabled in the async wrapper.
        Oracle: hand-counted invocation counts - 2 for the disabled package,
        1 for the sibling.
        """
        alpha_calls = []
        beta_calls = []

        @cachu.cache(ttl=300, backend='memory', package='alpha', tag='a')
        async def alpha(key: int) -> int:
            alpha_calls.append(key)
            return key

        @cachu.cache(ttl=300, backend='memory', package='beta', tag='b')
        async def beta(key: int) -> int:
            beta_calls.append(key)
            return key

        await alpha(1)
        await beta(1)
        cachu.disable(package='alpha')
        await alpha(1)
        await beta(1)

        assert len(alpha_calls) == 2
        assert len(beta_calls) == 1
