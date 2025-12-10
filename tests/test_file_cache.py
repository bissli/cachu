"""Test file cache backend operations.
"""
import os
import pathlib

import cache
import pytest


def test_file_cache_basic_decoration(temp_cache_dir):
    """Verify file cache decorator caches function results.
    """
    call_count = 0

    @cache.filecache(seconds=300).cache_on_arguments()
    def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = expensive_func(5)
    result2 = expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1


def test_file_cache_creates_dbm_file(temp_cache_dir):
    """Verify file cache creates DBM file in configured directory.
    """
    @cache.filecache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        return x * 2

    func(5)

    # With namespace isolation, files are prefixed with namespace (e.g., test_file_cache_cache5min)
    cache_files = [f for f in os.listdir(temp_cache_dir) if 'cache' in f]
    assert len(cache_files) > 0


@pytest.mark.parametrize(('seconds', 'expected_name'), [
    (30, 'cache30sec'),
    (120, 'cache2min'),
    (7200, 'cache2hour'),
])
def test_file_cache_naming_convention(seconds, expected_name):
    """Verify file cache uses correct naming convention for different expiration times.
    """
    @cache.filecache(seconds=seconds).cache_on_arguments()
    def func(x: int) -> int:
        return x

    func(1)

    from conftest import get_file_region
    region = get_file_region(seconds)
    assert expected_name in region.actual_backend.filename


def test_file_cache_persists_across_region_recreation(temp_cache_dir):
    """Verify file cache DBM file persists when region is deleted.
    """
    @cache.filecache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        return x * 2

    func(5)

    from conftest import get_file_region
    region = get_file_region(300)
    cache_file = region.actual_backend.filename

    assert pathlib.Path(cache_file).exists() or any(
        pathlib.Path(f'{cache_file}{ext}').exists()
        for ext in ['.db', '.dat', '.bak', '.dir']
    )

    # Remove the region entry (need to find the key with seconds=300)
    key_to_del = next((k for k in cache.cache._file_cache_regions if k[1] == 300), None)
    if key_to_del:
        del cache.cache._file_cache_regions[key_to_del]

    assert pathlib.Path(cache_file).exists() or any(
        pathlib.Path(f'{cache_file}{ext}').exists()
        for ext in ['.db', '.dat', '.bak', '.dir']
    )


def test_file_cache_with_namespace(temp_cache_dir):
    """Verify file cache namespace parameter is accepted.
    """
    @cache.filecache(seconds=300).cache_on_arguments(namespace='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id, 'name': 'test'}

    result = get_user(123)
    assert result['id'] == 123
