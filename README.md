# cache

Flexible caching library built on dogpile.cache with support for memory, file, and Redis backends.

## Installation

**Basic installation:**

```bash
# Using pip
pip install git+https://github.com/bissli/cache.git

# Using Poetry
poetry add git+https://github.com/bissli/cache.git
```

**With Redis support:**

```bash
# Using pip
pip install git+https://github.com/bissli/cache.git#egg=cache[redis]

# Using Poetry
poetry add git+https://github.com/bissli/cache.git -E redis
```

**In pyproject.toml:**

```toml
[tool.poetry.dependencies]
cache = {git = "https://github.com/bissli/cache.git"}

# Or with Redis support:
cache = {git = "https://github.com/bissli/cache.git", extras = ["redis"]}
```

**In requirements.txt:**

```
git+https://github.com/bissli/cache.git#egg=cache

# Or with Redis support:
git+https://github.com/bissli/cache.git#egg=cache[redis]
```

## Configuration

Configure the cache settings at application startup:

```python
import cache

cache.configure(
    debug_key="v1:",
    tmpdir="/var/cache/myapp"
)
```

**To enable memory caching**, you must explicitly set the memory backend:

```python
import cache

cache.configure(
    memory="dogpile.cache.memory_pickle",  # Required to enable memory cache
)
```

**To enable Redis caching**, you must explicitly set the Redis backend:

```python
import cache

cache.configure(
    redis="dogpile.cache.redis",  # Required to enable Redis
    redis_host="localhost",
    redis_port=6379,
    redis_db=0,
    redis_ssl=False,
    redis_distributed=False
)
```

Available configuration options:
- `debug_key`: Prefix for cache keys (default: "")
- `memory`: Backend for memory cache (default: "dogpile.cache.null", must be set to "dogpile.cache.memory_pickle" to enable memory caching)
- `redis`: Backend for redis cache (default: "dogpile.cache.null", must be set to "dogpile.cache.redis" to enable Redis)
- `redis_host`: Redis server hostname (default: "localhost")
- `redis_port`: Redis server port (default: 6379)
- `redis_db`: Redis database number (default: 0)
- `redis_ssl`: Use SSL for Redis connection (default: False)
- `redis_distributed`: Use distributed locks for Redis (default: False)
- `tmpdir`: Directory for file-based caches (default: "/tmp")
- `default_backend`: Backend for `defaultcache()` - "memory", "redis", or "file" (default: "memory")

### Namespace Isolation

Each calling package automatically gets its own isolated configuration. This prevents configuration conflicts when multiple libraries use the cache package with different settings:

```python
# In library_a/config.py
import cache
cache.configure(debug_key="lib_a:", redis_host="redis-a.example.com")

# In library_b/config.py
import cache
cache.configure(debug_key="lib_b:", redis_host="redis-b.example.com")

# Each library uses its own configuration automatically
```

New configurations inherit settings from the default, so you only need to specify what's different:

```python
# Set defaults for all packages
from cache.config import _registry, CacheConfig
_registry._default = CacheConfig(
    memory="dogpile.cache.memory_pickle",
    tmpdir="/var/cache/app"
)

# Package-specific overrides only need to specify changes
cache.configure(debug_key="mypackage:")
```

You can also retrieve configuration for a specific namespace:

```python
import cache

# Get config for the caller's namespace
cfg = cache.get_config()

# Get config for a specific namespace
cfg = cache.get_config(namespace="mypackage")
```

## Usage

### Memory Cache

```python
from cache import memorycache

@memorycache(seconds=300).cache_on_arguments()
def expensive_operation(param):
    # ... expensive computation
    return result
```

### File Cache

```python
from cache import filecache

@filecache(seconds=3600).cache_on_arguments()
def load_data(filename):
    # ... load and process file
    return data
```

### Redis Cache

```python
from cache import rediscache

@rediscache(seconds=86400).cache_on_arguments()
def fetch_external_data(api_key):
    # ... call external API
    return response
```

### Default Cache

Use `defaultcache` when you want to switch cache backends via configuration without changing code. This is useful for using memory cache in development and Redis in production:

```python
from cache import defaultcache

@defaultcache(seconds=300).cache_on_arguments()
def get_data(key):
    # ... fetch data
    return result
```

Configure which backend to use:

```python
import cache

# Development: use memory cache
cache.configure(
    default_backend="memory",
    memory="dogpile.cache.memory_pickle"
)

# Production: use Redis
cache.configure(
    default_backend="redis",
    redis="dogpile.cache.redis",
    redis_host="redis.example.com"
)
```

The `default_backend` option accepts "memory", "redis", or "file". All related operations work with the configured backend:

```python
from cache import (
    defaultcache,
    clear_defaultcache,
    set_defaultcache_key,
    delete_defaultcache_key
)

@defaultcache(seconds=300).cache_on_arguments(namespace="users")
def get_user(user_id):
    return fetch_user(user_id)

# These use whatever backend is configured
clear_defaultcache(seconds=300, namespace="users")
set_defaultcache_key(300, "users", get_user, {"id": 123}, user_id=123)
delete_defaultcache_key(300, "users", get_user, user_id=123)
```

**Safety feature**: If `rediscache()` is called but Redis is not configured (backend is null), it automatically falls back to `memorycache()` with a warning logged. This prevents silent failures in misconfigured environments.

### Namespaces

Namespaces organize cache keys into logical groups that can be selectively cleared:

```python
from cache import memorycache, rediscache

# Group related caches by namespace
@memorycache(seconds=300).cache_on_arguments(namespace="users")
def get_user_profile(user_id):
    return fetch_user_from_db(user_id)

@rediscache(seconds=3600).cache_on_arguments(namespace="api_data")
def fetch_weather(city):
    return call_weather_api(city)

# Clear specific namespace without affecting other cached data
clear_memorycache(seconds=300, namespace="users")
clear_rediscache(seconds=3600, namespace="api_data")
```

### Clearing Caches

Clear caches using optional `seconds` and `namespace` parameters:

```python
from cache import clear_memorycache, clear_filecache, clear_rediscache

# Clear specific region
clear_memorycache(seconds=300)
clear_filecache(seconds=3600)
clear_rediscache(seconds=86400)

# Clear specific namespace in a region
clear_memorycache(seconds=300, namespace="users")

# Clear all regions
clear_memorycache()

# Clear namespace across all regions
clear_memorycache(namespace="users")
```

**Clearing behavior:**

| seconds   | namespace   | Behavior                                                    |
| --------- | ----------- | ----------                                                  |
| `300`     | `None`      | Clears all keys in the 300-second region                    |
| `300`     | `"users"`   | Clears only "users" namespace keys in the 300-second region |
| `None`    | `None`      | Clears all keys in all regions                              |
| `None`    | `"users"`   | Clears "users" namespace keys across all regions            |

### Setting Specific Keys

Set cache values directly without calling the cached function:

```python
from cache import set_memorycache_key, set_filecache_key, set_rediscache_key

@memorycache(seconds=300).cache_on_arguments(namespace="users")
def get_user_profile(user_id):
    return fetch_user_from_db(user_id)

# Cache initial data
profile = get_user_profile(123)  # Cached

# Update cache directly without fetching from DB
updated_profile = {'id': 123, 'name': 'John Doe', 'email': 'john@example.com'}
set_memorycache_key(300, "users", get_user_profile, updated_profile, user_id=123)

# Next call returns updated data from cache
profile = get_user_profile(123)  # Returns updated_profile
```

**Multiple parameters:**

```python
@rediscache(seconds=86400).cache_on_arguments(namespace="analytics")
def get_metrics(user_id, metric_type, period="daily"):
    return calculate_metrics(user_id, metric_type, period)

# Set specific cached metrics directly
new_metrics = {'views': 1500, 'clicks': 250}
set_rediscache_key(
    86400,
    "analytics",
    get_metrics,
    new_metrics,
    user_id=123,
    metric_type="views",
    period="daily"
)
```

**When to use:**

- **Pre-warming cache**: Populate cache with computed values during off-peak hours
- **External updates**: Update cache when data changes through external means (webhooks, message queues)
- **Optimistic updates**: Update cache immediately with expected values before async operations complete
- **Batch updates**: Efficiently update multiple cache entries with pre-computed values

### Deleting Specific Keys

Delete individual cache entries by providing the exact parameters used when caching:

```python
from cache import delete_memorycache_key, delete_filecache_key, delete_rediscache_key

@memorycache(seconds=300).cache_on_arguments(namespace="users")
def get_user_profile(user_id):
    return fetch_user_from_db(user_id)

# Cache some data
profile = get_user_profile(123)  # Cached

# Delete specific cache entry when user profile is updated
delete_memorycache_key(300, "users", get_user_profile, user_id=123)

# Next call will fetch fresh data
profile = get_user_profile(123)  # Cache miss, fetches from DB
```

**Multiple parameters:**

```python
@rediscache(seconds=86400).cache_on_arguments(namespace="analytics")
def get_metrics(user_id, metric_type, period="daily"):
    return calculate_metrics(user_id, metric_type, period)

# Delete specific cached metrics
delete_rediscache_key(
    86400,
    "analytics",
    get_metrics,
    user_id=123,
    metric_type="views",
    period="daily"
)
```

**When to use:**

- **Single entry updates**: When specific data changes (e.g., user updates their profile)
- **Selective invalidation**: When you need to invalidate one cache entry without affecting others
- **Precise control**: When clearing an entire namespace or region would be too broad

**Key operations comparison:**

| Operation              | Scope                                    | Use Case                           |
| ---------------------- | ---------------------------------------- | ---------------------------------- |
| `set_*_key()`          | Single cache entry with exact parameters | Update cache with new value        |
| `delete_*_key()`       | Single cache entry with exact parameters | User updates their profile         |
| `clear_*(namespace=X)` | All keys in namespace across region(s)   | All user data needs refresh        |
| `clear_*(seconds=X)`   | All keys in specific time-based region   | Region-wide cache invalidation     |
| `clear_*()`            | All keys in all regions                  | Complete cache reset (development) |

## Intelligent Parameter Filtering

The library automatically filters out implementation details from cache keys, ensuring keys are based only on meaningful data parameters:

**Automatic filtering:**
- `self` and `cls` parameters (for instance and class methods)
- Database connection objects (detected using heuristics: objects with `driver_connection`, `dialect`, or `engine` attributes, or types containing 'Connection', 'Engine', 'psycopg', 'pyodbc', or 'sqlite3')
- Parameters starting with underscore (`_`)

**Instance methods:**

```python
from cache import memorycache

class UserRepository:
    def __init__(self, db_connection):
        self.conn = db_connection

    @memorycache(seconds=300).cache_on_arguments()
    def get_user(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        return cursor.fetchone()

# Both calls use the same cache entry (keyed only by user_id)
repo1 = UserRepository(connection1)
repo2 = UserRepository(connection2)
result = repo1.get_user(123)  # Cached
result = repo2.get_user(123)  # Cache hit
```

**Database connections:**

```python
@memorycache(seconds=300).cache_on_arguments()
def get_user_data(conn, user_id):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    return cursor.fetchone()

# Both calls use the same cache entry (keyed only by user_id)
result = get_user_data(connection1, 123)  # Cached
result = get_user_data(connection2, 123)  # Cache hit
```

## Features

- **Multiple backends**: Memory, file (DBM), and Redis support
- **Flexible expiration**: Configure different TTLs for different use cases
- **Namespace support**: Organize and selectively clear cache regions
- **Namespace isolation**: Each calling package gets isolated configuration automatically
- **Configurable default backend**: Switch between memory/file/Redis via configuration with `defaultcache`
- **Intelligent filtering**: Automatically excludes `self`, `cls`, database connections, and underscore-prefixed parameters
- **Custom key generation**: Smart key generation based on function signatures
- **Safe fallbacks**: Redis cache falls back to memory cache when Redis is not configured
