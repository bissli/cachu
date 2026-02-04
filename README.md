# cachu

Flexible caching library with support for memory, file, and Redis backends.

## Installation

**Basic installation:**

```bash
pip install cachu
```

**With Redis support:**

```bash
pip install cachu[redis]
```

## Quick Start

```python
import cachu

# Configure once at startup
cachu.configure(backend='memory', key_prefix='v1:')

# Use the @cache decorator
@cachu.cache(ttl=300)
def get_user(user_id: int) -> dict:
    return fetch_from_database(user_id)

# Cached automatically
user = get_user(123)  # Cache miss - fetches from DB
user = get_user(123)  # Cache hit - returns cached value
```

## Configuration

Configure cache settings at application startup:

```python
import cachu

cachu.configure(
    backend='memory',           # Default backend: 'memory', 'file', or 'redis'
    key_prefix='v1:',           # Prefix for all cache keys
    file_dir='/var/cache/app',  # Directory for file cache
    redis_url='redis://localhost:6379/0',  # Redis connection URL
)
```

### Configuration Options

| Option       | Default                      | Description                                                   |
| ------------ | ---------------------------- | ------------------------------------------------------------- |
| `backend`    | `'memory'`                   | Default backend: `'memory'`, `'file'`, `'redis'`, or `'null'` |
| `key_prefix` | `''`                         | Prefix for all cache keys (useful for versioning)             |
| `file_dir`   | `'/tmp'`                     | Directory for file-based caches                               |
| `redis_url`  | `'redis://localhost:6379/0'` | Redis connection URL (supports `rediss://` for TLS)           |

### Package Isolation

Each package automatically gets isolated configuration, preventing conflicts when multiple libraries use cachu:

```python
# In library_a/config.py
import cachu
cachu.configure(key_prefix='lib_a:', redis_url='redis://redis-a:6379/0')

# In library_b/config.py
import cachu
cachu.configure(key_prefix='lib_b:', redis_url='redis://redis-b:6379/0')

# Each library's @cache calls use its own configuration automatically
```

To override the automatic detection, specify the `package` parameter:

```python
from cachu import cache

# This function will use library_a's configuration
@cache(ttl=300, package='library_a')
def get_shared_data(id: int) -> dict:
    return fetch(id)
```

Retrieve configuration:

```python
cfg = cachu.get_config()                    # Current package's config
cfg = cachu.get_config(package='mylib')     # Specific package's config
all_configs = cachu.get_all_configs()       # All configurations
```

## Usage

### Basic Caching

```python
from cachu import cache

@cache(ttl=300, backend='memory')
def expensive_operation(param: str) -> dict:
    return compute_result(param)
```

### Backend Types

```python
# Memory cache (default)
@cache(ttl=300, backend='memory')
def fast_lookup(key: str) -> str:
    return fetch(key)

# File cache (persists across restarts)
@cache(ttl=3600, backend='file')
def load_config(name: str) -> dict:
    return parse_config_file(name)

# Redis cache (shared across processes)
@cache(ttl=86400, backend='redis')
def fetch_external_data(api_key: str) -> dict:
    return call_external_api(api_key)

# Null cache (passthrough, for testing)
@cache(ttl=300, backend='null')
def always_fresh(key: str) -> str:
    return fetch(key)  # Always executes, never caches
```

### Tags for Grouping

Tags organize cache entries into logical groups for selective clearing:

```python
from cachu import cache, cache_clear

@cache(ttl=300, tag='users')
def get_user(user_id: int) -> dict:
    return fetch_user(user_id)

@cache(ttl=300, tag='products')
def get_product(product_id: int) -> dict:
    return fetch_product(product_id)

# Clear only user caches
cache_clear(tag='users', backend='memory', ttl=300)
```

### Dynamic TTL

Use a callable to compute TTL based on the result:

```python
# TTL from result field
@cache(ttl=lambda result: result.get('cache_seconds', 300))
def get_config(key: str) -> dict:
    return fetch_config(key)  # Returns {'value': ..., 'cache_seconds': 600}

# Different TTL for different result types
def compute_ttl(result: dict) -> int:
    if result.get('is_stable'):
        return 3600  # Cache stable data for 1 hour
    return 60  # Cache volatile data for 1 minute

@cache(ttl=compute_ttl)
def get_data(id: int) -> dict:
    return fetch(id)
```

### Conditional Caching

Cache results only when a condition is met:

```python
# Don't cache None results
@cache(ttl=300, cache_if=lambda result: result is not None)
def find_user(email: str) -> dict | None:
    return db.find_by_email(email)

# Don't cache empty lists
@cache(ttl=300, cache_if=lambda result: len(result) > 0)
def search(query: str) -> list:
    return db.search(query)
```

### Validation Callbacks

Validate cached entries before returning:

```python
@cache(ttl=3600, validate=lambda entry: entry.age < 1800)
def get_price(symbol: str) -> float:
    # TTL is 1 hour, but recompute after 30 minutes
    return fetch_live_price(symbol)

# Validate based on value
def check_version(entry):
    return entry.value.get('version') == CURRENT_VERSION

@cache(ttl=86400, validate=check_version)
def get_config() -> dict:
    return load_config()
```

The `entry` parameter is a `CacheEntry` with:
- `value`: The cached value
- `created_at`: Unix timestamp when cached
- `age`: Seconds since creation

### Per-Call Control

Control caching behavior for individual calls:

```python
@cache(ttl=300)
def get_data(id: int) -> dict:
    return fetch(id)

# Normal call - uses cache
result = get_data(123)

# Skip cache for this call only (don't read or write cache)
result = get_data(123, _skip_cache=True)

# Force refresh - execute and overwrite cached value
result = get_data(123, _overwrite_cache=True)
```

### Decorator Helper Methods

Decorated functions have helper methods attached:

```python
@cache(ttl=300)
def get_user(user_id: int) -> dict:
    return fetch_user(user_id)

# .get() - retrieve cached value without calling the function
cached = get_user.get(user_id=123)           # Raises KeyError if not cached
cached = get_user.get(default=None, user_id=123)  # Returns None if not cached

# .set() - store a value directly in the cache
get_user.set({'id': 123, 'name': 'Test'}, user_id=123)

# .invalidate() - remove a specific entry from cache
get_user.invalidate(user_id=123)

# .refresh() - invalidate and re-fetch
user = get_user.refresh(user_id=123)

# .original() - call the original function, bypassing cache entirely
user = get_user.original(123)  # Always fetches, doesn't read or write cache
```

These methods also work with async functions:

```python
@cache(ttl=300)
async def get_user(user_id: int) -> dict:
    return await fetch_user(user_id)

cached = await get_user.get(user_id=123)
await get_user.set({'id': 123}, user_id=123)
await get_user.invalidate(user_id=123)
user = await get_user.refresh(user_id=123)
user = await get_user.original(123)
```

### Cache Statistics

Track hits and misses:

```python
from cachu import cache, cache_info

@cache(ttl=300)
def get_user(user_id: int) -> dict:
    return fetch_user(user_id)

# After some usage
info = cache_info(get_user)
print(f"Hits: {info.hits}, Misses: {info.misses}, Size: {info.currsize}")
```

### Excluding Parameters

Exclude parameters from the cache key:

```python
@cache(ttl=300, exclude={'logger', 'context'})
def process_data(logger, context, user_id: int, data: str) -> dict:
    logger.info(f"Processing for user {user_id}")
    return compute(data)

# Different logger/context values use the same cache entry
process_data(logger1, ctx1, 123, 'test')  # Cache miss
process_data(logger2, ctx2, 123, 'test')  # Cache hit
```

**Automatic filtering**: The library automatically excludes:
- `self` and `cls` parameters
- Parameters starting with underscore (`_`)
- Database connection objects

## CRUD Operations

### Direct Cache Manipulation

```python
from cachu import cache_get, cache_set, cache_delete, cache_clear

@cache(ttl=300, tag='users')
def get_user(user_id: int) -> dict:
    return fetch_user(user_id)

# Get cached value without calling function
user = cache_get(get_user, user_id=123, default=None)

# Set cache value directly
cache_set(get_user, {'id': 123, 'name': 'Updated'}, user_id=123)

# Delete specific cache entry
cache_delete(get_user, user_id=123)
```

### Clearing Caches

```python
from cachu import cache_clear

# Clear specific region
cache_clear(backend='memory', ttl=300)

# Clear by tag
cache_clear(tag='users', backend='memory', ttl=300)

# Clear all TTLs for a backend
cache_clear(backend='memory')

# Clear everything
cache_clear()
```

**Clearing behavior:**

| `ttl`  | `tag`     | `backend`  | Behavior                               |
| ------ | --------- | ---------- | -------------------------------------- |
| `300`  | `None`    | `'memory'` | All keys in 300s memory region         |
| `300`  | `'users'` | `'memory'` | Only "users" tag in 300s memory region |
| `None` | `None`    | `'memory'` | All memory regions                     |
| `None` | `'users'` | `None`     | "users" tag across all backends        |

### Cross-Module Clearing

When clearing from a different module, use the `package` parameter:

```python
# In myapp/service.py
@cache(ttl=300)
def get_data(id: int) -> dict:
    return fetch(id)

# In tests/conftest.py
from cachu import cache_clear
cache_clear(backend='memory', ttl=300, package='myapp')
```

## Instance and Class Methods

```python
class UserRepository:
    def __init__(self, db):
        self.db = db

    @cache(ttl=300)
    def get_user(self, user_id: int) -> dict:
        return self.db.fetch(user_id)

    @classmethod
    @cache(ttl=300)
    def get_default_user(cls) -> dict:
        return cls.DEFAULT_USER

    @staticmethod
    @cache(ttl=300)
    def get_guest() -> dict:
        return {'id': 0, 'name': 'Guest'}
```

## Testing

Disable caching globally for tests:

```python
import cachu
import pytest

@pytest.fixture(autouse=True)
def disable_caching():
    cachu.disable()
    yield
    cachu.enable()

# Check state
if cachu.is_disabled():
    print("Caching is disabled")
```

## Async Support

The library provides full async/await support with matching APIs:

```python
from cachu import async_cache, async_cache_get, async_cache_set, async_cache_delete
from cachu import async_cache_clear, async_cache_info

@async_cache(ttl=300, backend='memory')
async def get_user(user_id: int) -> dict:
    return await fetch_from_database(user_id)

# Usage
user = await get_user(123)  # Cache miss
user = await get_user(123)  # Cache hit

# Per-call control works the same way
user = await get_user(123, _skip_cache=True)
user = await get_user(123, _overwrite_cache=True)

# CRUD operations
cached = await async_cache_get(get_user, user_id=123)
await async_cache_set(get_user, {'id': 123, 'name': 'Test'}, user_id=123)
await async_cache_delete(get_user, user_id=123)
await async_cache_clear(backend='memory', ttl=300)

# Statistics
info = await async_cache_info(get_user)
```

All decorator options (`ttl`, `backend`, `tag`, `exclude`, `cache_if`, `validate`, `package`) work identically to the sync version.

## Advanced

### Direct Backend Access

```python
from cachu import get_backend

backend = get_backend('memory', ttl=300)
backend.set('my_key', {'data': 'value'}, ttl=300)
value = backend.get('my_key')
backend.delete('my_key')
```

### Redis Client Access

```python
from cachu import get_redis_client

client = get_redis_client()
client.set('direct_key', 'value')
```

## Public API

```python
from cachu import (
    # Configuration
    configure,
    get_config,
    get_all_configs,
    disable,
    enable,
    is_disabled,

    # Sync Decorator
    cache,

    # Sync CRUD Operations
    cache_get,
    cache_set,
    cache_delete,
    cache_clear,
    cache_info,

    # Async Decorator
    async_cache,

    # Async CRUD Operations
    async_cache_get,
    async_cache_set,
    async_cache_delete,
    async_cache_clear,
    async_cache_info,

    # Advanced
    get_backend,
    get_async_backend,
    get_redis_client,
    Backend,
    AsyncBackend,
    clear_async_backends,
)
```

## Features

- **Multiple backends**: Memory, file (SQLite), Redis, and null (passthrough)
- **Async support**: Full async/await API with `@async_cache` decorator
- **Flexible TTL**: Static or dynamic TTL (callable that receives result)
- **Tags**: Organize and selectively clear cache entries
- **Package isolation**: Each package gets isolated configuration
- **Conditional caching**: Cache based on result value
- **Validation callbacks**: Validate entries before returning
- **Per-call control**: Skip or overwrite cache per call
- **Helper methods**: `.get()`, `.set()`, `.invalidate()`, `.refresh()`, `.original()` on decorated functions
- **Statistics**: Track hits, misses, and cache size
- **Intelligent filtering**: Auto-excludes `self`, `cls`, connections, and `_` params
- **Global disable**: Bypass all caching for testing
- **Redis TLS**: Supports `rediss://` URLs for secure connections
