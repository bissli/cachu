"""Tests that every shipped backend is discoverable by introspection.

Notes
-----
- 'null' has always been a valid backend and works correctly, but it
  appeared only in one docstring's argument list while cachu.backends
  exposed modules for memory, redis and sqlite.
- Adopters concluded it was unavailable and reached for ttl=0 instead.
- These pin the wiring so a backend cannot become functionally present but
  invisible again.
"""
import subprocess
import sys

import cachu
from cachu.backends import DynamoDBBackend, MemoryBackend, NullBackend
from cachu.backends import RedisBackend, SqliteBackend
from cachu.config import VALID_BACKENDS


class TestBackendsPackageExports:
    """cachu.backends names every backend a caller can select.
    """

    def test_every_valid_backend_name_has_an_exported_module(self):
        """Each name accepted by configure() has a module under cachu.backends.

        Mutation: add a backend to VALID_BACKENDS without exporting it, or
        drop 'null' from the package exports again.
        Oracle: VALID_BACKENDS itself, mapped through the name each module
        uses ('file' is served by the sqlite module).
        """
        module_for_backend = {
            'memory': 'memory',
            'file': 'sqlite',
            'redis': 'redis',
            'null': 'null',
            'dynamodb': 'dynamodb',
        }

        assert set(module_for_backend) == set(VALID_BACKENDS)
        for module_name in module_for_backend.values():
            assert hasattr(cachu.backends, module_name)
            assert module_name in cachu.backends.__all__

    def test_backend_classes_are_importable_from_the_package(self):
        """The concrete classes are reachable without a submodule path.

        Mutation: export the modules but not the classes.
        Oracle: the five documented backend classes.
        """
        for cls in (DynamoDBBackend, MemoryBackend, NullBackend, RedisBackend,
                    SqliteBackend):
            assert cls.__name__ in cachu.backends.__all__
            assert getattr(cachu.backends, cls.__name__) is cls

    def test_backends_package_is_reachable_from_a_bare_import(self):
        """`import cachu` alone exposes cachu.backends and every backend class.

        Mutation: drop `backends` from cachu.__all__, or stop exporting a
        backend class from cachu/backends/__init__.
        Oracle: the documented export list, resolved in a FRESH interpreter
        that imports nothing but cachu.

        Deleting the `from . import backends` line specifically is NOT
        detectable and no assertion here claims otherwise: cachu/__init__
        also does `from .backends.redis import get_redis_client`, which
        binds the submodule attribute as a side effect. The line is kept as
        the intentional declaration rather than relying on that accident -
        which is the coupling that made the null backend invisible in the
        first place - but the two are behaviorally identical.
        """
        probe = (
            'import cachu; '
            "assert 'backends' in cachu.__all__, 'backends missing from __all__'; "
            "names = ['DynamoDBBackend', 'MemoryBackend', 'NullBackend', "
            "'RedisBackend', 'SqliteBackend']; "
            'assert all(hasattr(cachu.backends, n) for n in names), '
            'sorted(cachu.backends.__all__); '
            "assert hasattr(cachu, 'create_dynamodb_table'), 'table helper missing'; "
            'print("ok")'
        )
        result = subprocess.run(
            [sys.executable, '-c', probe],
            capture_output=True, text=True, timeout=60)

        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == 'ok'
        assert cachu.backends.NullBackend is NullBackend

    def test_table_helper_is_exported_top_level(self):
        """cachu.create_dynamodb_table is reachable from the top-level package.

        Mutation: drop the export, leaving the README's documented setup
        call an AttributeError.
        Oracle: the documented top-level name, identical to the backend
        module's function.
        """
        assert 'create_dynamodb_table' in cachu.__all__
        assert cachu.create_dynamodb_table is cachu.backends.dynamodb.create_dynamodb_table


class TestNullBackendIsSelectable:
    """'null' behaves as the documented per-cache off switch.
    """

    def test_null_is_a_valid_configured_default(self):
        """configure(backend_default='null') is accepted.

        Mutation: drop 'null' from VALID_BACKENDS, which would reject the
        documented way to switch one cache off.
        Oracle: the configured value round-tripped through get_config.
        """
        cachu.configure(backend_default='null')

        assert cachu.get_config().backend_default == 'null'

    def test_null_backend_switches_off_one_cache_only(self):
        """A 'null' cache re-executes while its sibling keeps caching.

        Mutation: make 'null' fall back to the memory backend.
        Oracle: hand-counted invocation counts - 2 for the null-backed
        function, 1 for the memory-backed one.
        """
        off_calls = []
        on_calls = []

        @cachu.cache(ttl=300, backend='null', tag='off')
        def switched_off(key: int) -> int:
            off_calls.append(key)
            return key

        @cachu.cache(ttl=300, backend='memory', tag='on')
        def switched_on(key: int) -> int:
            on_calls.append(key)
            return key

        switched_off(1)
        switched_off(1)
        switched_on(1)
        switched_on(1)

        assert len(off_calls) == 2
        assert len(on_calls) == 1


class TestPublicExceptionExports:
    """Exception types callers must catch are exported.
    """

    def test_exception_hierarchy_is_public(self):
        """The library's exceptions are importable from the top level.

        Mutation: leave CacheLockTimeout reachable only via cachu.exception,
        so the documented `except cachu.CacheLockTimeout` fails.
        Oracle: the documented names and their common base class.
        """
        for name in ('CacheError', 'CacheLockTimeout',
                     'ConfigurationError', 'BackendNotFoundError'):
            assert name in cachu.__all__
            assert issubclass(getattr(cachu, name), Exception)

        assert issubclass(cachu.CacheLockTimeout, cachu.CacheError)

    def test_cache_config_is_public(self):
        """CacheConfig is exported so defaults can be inspected.

        Mutation: keep CacheConfig private, forcing callers into
        cachu.config internals to read a default.
        Oracle: the dataclass returned by get_config().
        """
        assert 'CacheConfig' in cachu.__all__
        assert isinstance(cachu.get_config(), cachu.CacheConfig)
