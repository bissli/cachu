"""Test cache behavior with null backend configuration.

When a cache backend is set to 'dogpile.cache.null', the corresponding
cache function should create a null region (no-op caching), not fall back
to a different cache type.
"""
import cache

# =============================================================================
# rediscache() with null backend
# =============================================================================


def test_rediscache_with_null_backend_creates_null_region():
    """Verify rediscache() creates null region when redis backend is null."""
    from conftest import get_redis_region

    @cache.rediscache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        return x * 2

    func(5)

    region = get_redis_region(300)
    assert region is not None
    # Verify it's a null backend, not memory
    assert region._region.backend.__class__.__name__ == 'NullBackend'


def test_rediscache_with_null_backend_does_not_cache():
    """Verify null backend doesn't actually cache values."""
    call_count = 0

    @cache.rediscache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = func(5)
    result2 = func(5)

    assert result1 == 10
    assert result2 == 10
    # Function called twice because null backend doesn't cache
    assert call_count == 2


def test_rediscache_with_null_backend_does_not_create_memory_region():
    """Verify rediscache() with null backend doesn't fall back to memory."""
    from conftest import has_memory_region

    @cache.rediscache(seconds=600).cache_on_arguments()
    def func(x: int) -> int:
        return x * 2

    func(5)

    # Should NOT have created a memory region as fallback
    assert not has_memory_region(600)


def test_rediscache_with_null_backend_region_reuse():
    """Verify null regions are properly reused for same TTL."""
    from conftest import get_redis_region

    @cache.rediscache(seconds=300).cache_on_arguments()
    def func1(x: int) -> int:
        return x * 2

    @cache.rediscache(seconds=300).cache_on_arguments()
    def func2(x: int) -> int:
        return x * 3

    func1(5)
    func2(5)

    # Both should share the same region
    region = get_redis_region(300)
    assert region is not None


def test_rediscache_with_null_backend_different_ttls():
    """Verify different TTLs create separate null regions."""
    from conftest import get_redis_region

    @cache.rediscache(seconds=100).cache_on_arguments()
    def func1(x: int) -> int:
        return x * 2

    @cache.rediscache(seconds=200).cache_on_arguments()
    def func2(x: int) -> int:
        return x * 3

    func1(5)
    func2(5)

    region1 = get_redis_region(100)
    region2 = get_redis_region(200)

    assert region1 is not None
    assert region2 is not None
    assert region1 is not region2


# =============================================================================
# memorycache() with null backend
# =============================================================================

def test_memorycache_with_null_backend_creates_null_region():
    """Verify memorycache() creates null region when memory backend is null."""
    from conftest import get_memory_region

    # Configure memory to null
    cache.configure(memory='dogpile.cache.null')

    @cache.memorycache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        return x * 2

    func(5)

    region = get_memory_region(300)
    assert region is not None
    assert region._region.backend.__class__.__name__ == 'NullBackend'


def test_memorycache_with_null_backend_does_not_cache():
    """Verify memorycache with null backend doesn't cache values."""
    cache.configure(memory='dogpile.cache.null')

    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = func(5)
    result2 = func(5)

    assert result1 == 10
    assert result2 == 10
    # Function called twice because null backend doesn't cache
    assert call_count == 2


# =============================================================================
# filecache() with null backend
# =============================================================================

def test_filecache_with_null_backend_creates_null_region():
    """Verify filecache() creates null region when file backend is null."""
    from conftest import get_file_region

    # Configure file to null
    cache.configure(file='dogpile.cache.null')

    @cache.filecache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        return x * 2

    func(5)

    region = get_file_region(300)
    assert region is not None
    assert region._region.backend.__class__.__name__ == 'NullBackend'


def test_filecache_with_null_backend_does_not_cache():
    """Verify filecache with null backend doesn't cache values."""
    cache.configure(file='dogpile.cache.null')

    call_count = 0

    @cache.filecache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = func(5)
    result2 = func(5)

    assert result1 == 10
    assert result2 == 10
    # Function called twice because null backend doesn't cache
    assert call_count == 2


def test_filecache_with_null_backend_does_not_create_dbm_file(temp_cache_dir):
    """Verify filecache with null backend doesn't create any files."""
    import os

    cache.configure(file='dogpile.cache.null')

    @cache.filecache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        return x * 2

    func(5)

    # Should not have created any cache files
    files = os.listdir(temp_cache_dir)
    cache_files = [f for f in files if 'cache' in f]
    assert len(cache_files) == 0
