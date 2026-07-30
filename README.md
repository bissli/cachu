# cachu
*pronunciation: ka-SHOO*

Flexible caching library with support for memory, file, Redis, and null backends.

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
cachu.configure(backend_default='memory', key_prefix='v1:')

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
    backend_default='memory',   # Default backend: 'memory', 'file', 'redis', or 'null'
    key_prefix='v1:',           # Prefix for all cache keys
    file_dir='/var/cache/app',  # Directory for file cache
    redis_url='redis://localhost:6379/0',  # Redis connection URL
)
```

### Configuration Options

| Option                        | Default                      | Description                                                                                                                                                                   |
| ----------------------------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `backend_default`             | `'memory'`                   | Default backend: `'memory'`, `'file'`, `'redis'`, or `'null'`                                                                                                                 |
| `key_prefix`                  | `''`                         | Prefix for all cache keys (useful for versioning)                                                                                                                             |
| `file_dir`                    | `'/tmp'`                     | Directory for file-based caches                                                                                                                                               |
| `redis_url`                   | `'redis://localhost:6379/0'` | Redis connection URL (supports `rediss://` for TLS)                                                                                                                           |
| `package`                     | caller's package             | Which package's configuration to set; a parameter, not a stored field ([details](#package-isolation))                                                                         |
| `fail_open`                   | `True`                       | Degrade cache faults to a miss instead of raising ([details](#failure-semantics))                                                                                             |
| `cache_deadline`              | `None`                       | Cumulative cache work per call, checked *between* backend operations - it cannot interrupt one already in flight ([details](#bounding-cache-latency))                         |
| `lock_timeout`                | `10.0`                       | Seconds to wait for the per-key dogpile mutex; **lowering it increases** backing-store load under the default `on_lock_timeout='run'` ([details](#dogpile-and-lock-timeouts)) |
| `on_lock_timeout`             | `'run'`                      | `'run'` or `'raise'` when the mutex is missed ([details](#dogpile-and-lock-timeouts))                                                                                         |
| `memory_maxsize`              | `None`                       | LRU bound for the memory backend ([details](#bounding-the-memory-backend))                                                                                                    |
| `memory_sweep_interval`       | `60.0`                       | Seconds between expired-entry sweeps of the memory backend ([details](#bounding-the-memory-backend))                                                                          |
| `redis_socket_timeout`        | `5.0`                        | Socket timeout, applied to **both** connect and read; the only thing that bounds one in-flight operation ([details](#bounding-cache-latency))                                 |
| `redis_retry_count`           | `3`                          | redis-py retries per operation - they run *inside* one operation, so they multiply its worst case ([details](#bounding-cache-latency))                                        |
| `redis_health_check_interval` | `30`                         | Seconds between redis-py connection health checks                                                                                                                             |

`configure()` only changes the settings you pass, and `None` means "leave unchanged" -
so an option whose default is `None` cannot be reset through the public API once set.
`file_dir` is validated eagerly and must already exist and be writable. An invalid
setting raises `ConfigurationError`, which subclasses both `CacheError` and `ValueError`.

### When Each Setting Takes Effect

Most settings are read on every call, but two groups are not. Configure before the first
cached call and the second group never bites you; `backend_default` is resolved when the
decorator *runs*, so it must be set before the module holding the `@cache` is imported -
otherwise name the backend on the decorator.

| Read when                                | Settings                                                                                                                                                                                                    |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Decoration (import time)                 | `backend_default`                                                                                                                                                                                           |
| Backend construction (first cached call) | `redis_url`, `redis_socket_timeout`, `redis_retry_count`, `redis_health_check_interval`, `file_dir`, `memory_maxsize`, `memory_sweep_interval`, and `lock_timeout` as the Redis lock key's self-heal expiry |
| Every call                               | `key_prefix`, `fail_open`, `cache_deadline`, `on_lock_timeout`, and `lock_timeout` as the wait length                                                                                                       |

Changing a construction-time setting after a backend exists has no effect on that
backend. `cachu.clear_backends()` forces reconstruction if you need it.

### Using Multiple Backends

You only need one `configure()` call even when using different backends across your application.
The `configure()` function sets shared settings and a default backend. Individual decorators
can override the backend:

```python
import cachu

# Configure shared settings once at startup
cachu.configure(
    backend_default='memory',             # Default backend
    redis_url='redis://myserver:6379/0',  # Used when backend='redis'
    file_dir='/var/cache/app',            # Used when backend='file'
    key_prefix='v1:'                      # Applied to all backends
)

# Use different backends per-function
@cachu.cache(ttl=60)                      # Uses default (memory)
def get_session(session_id: str) -> dict:
    return fetch_session(session_id)

@cachu.cache(ttl=3600, backend='file')    # Uses file backend
def get_config(name: str) -> dict:
    return load_config(name)

@cachu.cache(ttl=86400, backend='redis')  # Uses redis backend
def get_user(user_id: int) -> dict:
    return fetch_user(user_id)
```

**Key points:**
- `redis_url` is used whenever `backend='redis'` is specified
- `file_dir` is used whenever `backend='file'` is specified
- `key_prefix` applies to all backends
- The `backend_default` in `configure()` is just the default when not specified in the decorator

### Package Isolation

The `package` parameter selects which configuration your `@cache` calls use, so multiple
libraries sharing `cachu` never collide.

**How auto-detection works:** When `package` is not specified, cachu walks the call stack
and takes the top-level package name from the caller's `__name__`. For example, if
`@cache` is applied inside `mylib.utils.foo`, the resolved package is `mylib`. When the
caller is `__main__`, cachu uses the script filename instead (e.g. `__main__.app`).

```python
# In library_a/config.py
import cachu
cachu.configure(key_prefix='lib_a:', redis_url='redis://redis-a:6379/0')

# In library_b/config.py
import cachu
cachu.configure(key_prefix='lib_b:', redis_url='redis://redis-b:6379/0')

# Each library's @cache calls use its own configuration automatically
```

**When to use explicit `package=`:** Use it when your code might be imported from
different packages (vendored, bundled), or when you want deterministic behavior
regardless of call context:

```python
from cachu import cache

# This function will always use library_a's configuration
@cache(ttl=300, package='library_a')
def get_shared_data(id: int) -> dict:
    return fetch(id)
```

**Splitting one package into several config scopes:** `backend=` on the decorator is
per-cache, but every timeout and budget is per-package. Pass `package=` to `configure()`
to give one latency-sensitive cache its own settings without touching the rest of your
application:

```python
import cachu

# One authorization cache on the request path: fail fast.
cachu.configure(
    package='myapp.authz',
    redis_socket_timeout=0.25,
    cache_deadline=1.0,
)

# Everything else in myapp keeps the defaults.
cachu.configure(package='myapp', redis_url='redis://cache:6379/0')

@cachu.cache(ttl=60, package='myapp.authz', backend='redis')
def is_authorized(token_hash: str) -> bool:
    return registry_lookup(token_hash)
```

The package name is just a key, so `'myapp.authz'` above is a scope label rather than an
importable module. Auto-detection only ever produces top-level names, so a dotted label
cannot be claimed accidentally by another caller.

**Debugging:** Enable `DEBUG` logging on the `cachu` logger to see which package and
backend each decorated function resolved to:

```python
import logging
logging.getLogger('cachu').setLevel(logging.DEBUG)
```

Example output:

```
DEBUG:cachu.decorator:@cache get_user: package='mylib', backend='memory', ttl=300
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

cachu ships four backends. All are importable from `cachu.backends` for introspection.

| Name       | Class           | Scope             | Use for                                        |
| ---------- | --------------- | ----------------- | ---------------------------------------------- |
| `'memory'` | `MemoryBackend` | This process      | Hot lookups; optionally LRU-bounded            |
| `'file'`   | `SqliteBackend` | This machine      | Results worth surviving a restart              |
| `'redis'`  | `RedisBackend`  | Every process     | Shared state across workers or hosts           |
| `'null'`   | `NullBackend`   | Nothing is stored | Switching one cache off, and passthrough tests |

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

# Null cache (passthrough) - always executes, never caches
@cache(ttl=300, backend='null')
def always_fresh(key: str) -> str:
    return fetch(key)
```

`backend='null'` is the way to express "this cache is switched off" for one function.
It is a real backend, not a testing hack: the decorator, its helper methods and
`cache_clear` all keep working, they simply never store anything. Prefer it over
`ttl=0` (which relies on a non-positive TTL being treated as uncacheable) and over
`cachu.disable()`, which is process-wide unless you scope it.

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

#### Args-aware TTL

`ttl` callables can also accept a second positional parameter and receive
the filtered args dict — useful when freshness depends on the request
shape, not the result. The args dict is the same view used to build the
cache key (with `self`/`cls`/`_`-prefixed/`exclude=`d/connection-like
values dropped):

```python
import datetime

# Short TTL for today, long TTL for past dates
@cache(ttl=lambda result, args: 900 if args['date'] == datetime.date.today() else 86400)
def get_filings(date: datetime.date) -> list:
    return fetch_filings(date)
```

Arity is detected once at decoration time via `inspect.signature`. A
predicate written as `def f(result, args=None)` is treated as 2-arg, so
you can opt in without changing call sites. A predicate with 0 or >2
required positional params raises `TypeError` at decoration.

### Conditional Caching

Cache results only when a condition is met. `cache_if` runs after the
function call; returning `False` bypasses the write but does not affect
the read. **Concurrent callers that all hit a `cache_if=False` path will
each re-fetch** — the per-key mutex protects the read/write race, not
the predicate decision.

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

#### Args-aware cache_if

`cache_if` accepts the same 2-arg overload as `ttl`. The args dict lets
you gate caching on the call shape, not just the result — for example,
suppress caching of empty results only for "today's" date while keeping
the empty cache for historical dates (where empty is usually the final
answer):

```python
import datetime

@cache(
    ttl=300,
    cache_if=lambda result, args: bool(result) or args['date'] != datetime.date.today(),
)
def get_filings(date: datetime.date) -> list:
    return fetch_filings(date)
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

`validate` also accepts a 2-arg `validate(entry, args)` form when you
need the call shape to influence the staleness decision (e.g. require a
shorter age window for today vs historical dates).

### Presets

`cachu.presets` ships ready-made predicate bundles for common
args-aware patterns. Each preset returns a dict of decorator kwargs to
splat into `@cache(...)`.

#### today_aware

For date-keyed fetches where "today" is volatile (more data arrives
throughout the day) but past dates are immutable. Short TTL for today,
long TTL for past dates, and (by default) empty results for today are
not cached so a transient empty does not pin the cache. Empty results
for past dates ARE cached, since historical empties are typically final.

```python
import datetime
from cachu import cache, presets

@cache(
    tag='filings',
    **presets.today_aware(
        date_param='date',
        today_ttl=900,      # 15 min
        past_ttl=86400,     # 24 h
    ),
)
def get_filings(date: datetime.date) -> list:
    return fetch_filings(date)
```

`today_ttl` and `past_ttl` are required so each call site makes a
deliberate freshness decision. Optional knobs: `skip_empty_today=True`
(default), `skip_empty_past=False` (default), `today_fn=datetime.date.today`
(injectable for tests).

The preset raises `KeyError` with a clear message if `date_param` is
not found in the args dict — usually a sign that the parameter was
renamed or removed by `exclude=`.

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

# .clear() - remove a specific entry from cache
get_user.clear(user_id=123)

# .refresh() - clear and re-fetch
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
await get_user.clear(user_id=123)
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

**Clearing works in a cold process.** `@cache` registers its
`(package, backend, ttl)` region when the decorator runs, which is import time, so
`cache_clear` can reach a region even if no cached call has happened yet. This matters
most in tests: a setup fixture that clears against a shared Redis or SQLite backend
really clears it, instead of silently no-opping and letting a previous run's value be
served.

That reach has a cost: an unscoped `cache_clear()` instantiates every declared region,
so it creates the SQLite file for any `backend='file'` region and connects to any
`backend='redis'` region - spending that backend's full socket budget if it is
unreachable. A failure on a backend you did not name is logged and skipped rather than
raised, but the time is still spent. Scope the call
(`cache_clear(backend='memory', ttl=300)`) if you do not want that.

A return of `0` means "no entries matched". If no region matched at all - usually a
misspelled `package` or `backend` - a warning is logged on the `cachu.operations`
logger, so the two cases stay distinguishable.

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

## Reliability and Latency

A cache is an optimization. On a request path it should only ever be able to cost you
speed - never the answer, and never an unbounded amount of time. This section covers the
settings that make that true, and the three places where the default is deliberately the
pre-0.4 behaviour rather than the safe one.

**Those unsafe defaults are deliberate.** `memory_maxsize` (`None`), `cache_deadline`
(`None`) and `on_lock_timeout` (`'run'`) all keep the historical behaviour, because
turning any of them on by default would change the outcome of an existing unmodified
call - a new eviction, a new skipped write, or a new escaping exception. Turn on
`memory_maxsize` when callers influence the key space, `cache_deadline` when the caller
has a deadline, and `on_lock_timeout='raise'` when you would rather shed than stampede.

### Failure Semantics

With `fail_open=True` (the default) no cache fault reaches your caller. Building the
cache key and constructing the backend degrade to running the decorated function
uncached. A read fault, a mutex-construction fault or a failed lock acquire instead
degrade to a **miss**: the function runs, and its result is still written to the cache
and still counted in the stats.

```python
cachu.configure(backend_default='redis', redis_url='redis://unreachable:6379/0')

@cachu.cache(ttl=60, tag='authz')
def is_authorized(token: str) -> bool:
    return registry_lookup(token)

is_authorized('abc')   # returns the real answer; the cache fault is logged, not raised
```

Set `fail_open=False` to make those **read-path** faults propagate instead - appropriate
when a cache miss is more expensive than an error.

**Writes, stat updates and lock release are always best-effort**, whichever way
`fail_open` is set: they run after the result already exists, so failing the call would
throw away a correct answer over a cache-only problem. They are logged, never raised.

**A stored value that no longer decodes is a miss, and is logged.** The usual cause is a
deploy that changes a pickled class while an older release still writes the same key,
which drives the hit rate to zero for as long as both run. Both the Redis and the file
backend evict the row and warn (`Evicting undecodable cache row for key ...`), so the
condition is visible rather than silent.

**`fail_open` bounds exceptions, not hangs.** A wedged endpoint - a blackholed address
rather than a refused connection - blocks inside socket timeouts and never raises, so
neither `fail_open` nor a `try`/`except` around the call can shorten it. Neither can
`cache_deadline` on its own: it is checked only *between* backend operations, so a call
already blocked in a socket read runs to completion. Only `redis_socket_timeout` and
`redis_retry_count` bound a single in-flight operation - see
[Bounding Cache Latency](#bounding-cache-latency).

**The one deliberate exception** is `on_lock_timeout='raise'`: `CacheLockTimeout`
propagates even under `fail_open=True`, because shedding load is a decision you opted
into rather than a fault. It also escapes `_overwrite_cache=True` and `.refresh()`.

The helper methods (`.get()`, `.set()`, `.clear()`) and the module-level CRUD functions
are explicit cache operations, not cached calls: they are governed by neither
`fail_open` nor `cache_deadline` and report backend errors directly. `.refresh()` is the
exception - it clears and then makes a real cached call, so its second half obeys both.

### Bounding Cache Latency

Redis timeout budgets compound rather than add:

- `redis_socket_timeout` applies to **both** the connect and the read.
- redis-py retries each operation `redis_retry_count` times with exponential backoff,
  and those retries run *inside* one logical operation rather than around it.
- A miss performs six Redis round trips: get, mutex acquire (`SET NX`), the post-lock
  re-read, stat increment, set, mutex release. A hit performs two.
- Against a blackholed endpoint the acquire *raises* rather than polling, so the release
  never runs and the miss costs five full socket budgets. `lock_timeout` contributes
  nothing to that number - it bounds contention, not an outage.

With the defaults (`redis_socket_timeout=5.0`, `redis_retry_count=3`), a single cached
call against a blackholed endpoint has been measured at **100.7 seconds** - five
operations at `5.0 * 4`. It returned the correct value via `fail_open`, but a
100-second cache lookup is indistinguishable from an outage to any caller with a
deadline.

`cache_deadline` bounds the total cache-attributable work in one decorated call:

```python
cachu.configure(cache_deadline=1.0)
```

Once the budget is spent, the remaining cache steps are skipped and the function runs
uncached. Specifically:

- Reads, stat increments and the write are skipped once it is exhausted. Stats are
  best-effort, so a cache thrashing under an exhausted budget reports no hits and no
  misses.
- **Time spent inside your function does not count.** A function slower than the
  deadline is still cached; only cache work spends the budget.
- **Nor does time spent waiting for another caller's function.** A dogpile waiter is
  watching someone else's copy of the same work, so the wait is refunded exactly as
  the caller's own runtime is. `lock_timeout` bounds that wait; `cache_deadline` does
  not. The two knobs are orthogonal - one bounds waiting for a peer, the other bounds
  cachu's own I/O - so a call can outlive its deadline by up to `lock_timeout` when it
  is queued behind a slow producer.

**`cache_deadline` alone is not enough for Redis.** The budget is only checked *between*
steps, so a call already blocked in a socket read runs to completion - and redis-py puts
its retries *inside* one operation. The mutex release in the `finally` is unconditional
too (skipping it would leak the lock), so when the lock was held two uninterruptible
operations can stack on one call:

    T = redis_socket_timeout * (1 + redis_retry_count)

    no lock held:  worst case ~= cache_deadline + T
    lock held:     worst case ~= cache_deadline + 2*T

With the shipped defaults T is `5.0 * 4 = 20s`, so `cache_deadline=1.0` by itself still
admits a 21-second call, or 41 seconds when the lock was held. Treat `T` as a floor
rather than an exact figure: redis-py adds backoff sleeps between retries, a
health-checked connection can spend an extra round trip on a `PING`, and the retry
semantics differ across the `redis>=4.2.0` range cachu accepts. cachu logs a warning the
first time it builds a Redis backend for a package whose deadline the Redis budgets
cannot honour - on the first Redis-backed call, not inside `configure()`, and not at all
for a package that never touches Redis. Set all three together:

```python
cachu.configure(
    package='myapp.authz',
    cache_deadline=1.0,
    redis_socket_timeout=0.25,
    redis_retry_count=1,
)   # T = 0.5s; worst case ~= 1.5s, or ~= 2.0s with the lock held
```

`redis_retry_count=0` is the one setting that makes the arithmetic exact, since the
retries are what compound.

cachu deliberately does **not** derive `redis_socket_timeout` from `cache_deadline` for
you. Doing so was measured to override an explicitly configured value and, against a
healthy but slow endpoint, to time out every read and write - turning the cache into a
100% miss that `fail_open` then hid. Choosing how much latency to trade for hit rate is
yours to make.

**Do not set `cache_deadline` below your backend's round trip.** The read is attempted
first and can spend the whole budget on its own, which then skips the write - so the
entry is never stored, every later call misses and pays the same slow read again, and
the cache can never populate. A cache configured that way is slower than no cache at
all. The skipped write logs a warning naming exactly this.

### Dogpile and Lock Timeouts

cachu suppresses dogpiles with a per-key mutex: on a miss, one caller computes and the
rest wait, then read the value the winner stored.

When a waiter cannot take the mutex within `lock_timeout`, the default
`on_lock_timeout='run'` executes the function anyway. **Lowering `lock_timeout` to shed
load therefore has the opposite effect** - each waiter that gives up becomes its own
backing-store read. Measured with a 2.0s store and 6 concurrent same-key requests:

| `lock_timeout` | `on_lock_timeout` | store reads | shed callers | p100 latency               |
| -------------- | ----------------- | ----------- | ------------ | -------------------------- |
| `10.0`         | `'run'` (default) | 1           | 0            | 2.00 s                     |
| `1.0`          | `'run'`           | 6           | 0            | 3.00 s                     |
| `1.0`          | `'raise'`         | 1           | 5            | 2.00 s winner, 1.00 s shed |

Note the third row: `'raise'` sheds the five waiters at 1.00 s, but the lock winner
still pays the full 2.00 s to populate the cache.

To shed load instead of stampeding, opt into raising:

```python
cachu.configure(lock_timeout=1.0, on_lock_timeout='raise')

try:
    data = get_data(key)
except cachu.CacheLockTimeout:
    return SERVICE_BUSY
```

A waiter whose wait was rewarded still gets the value: the re-read after the lock
attempt happens first, and only a genuine miss raises.

**Only a real, failed wait sheds.** Two things that are not a lock timeout and never
raise: a lock *error* under `fail_open=True`, which degrades to running without the
lock; and an exhausted `cache_deadline`, which skips the acquire entirely. Shedding a
caller that never attempted the lock would mean a cache merely slower than its budget
sheds every call - the function would never run, so nothing would ever be stored, so
nothing would recover.

`'raise'` also stops shedding during a backend outage: a mutex whose `acquire` raises
is a fault, not a timeout, so every caller runs the function. Load shedding protects
you from your own traffic, not from a broken cache.

`CacheLockTimeout` subclasses `cachu.CacheError`. If you catch `CacheError` broadly and
re-run the function yourself, exclude this one - otherwise you turn the shedding back
into the stampede.

### Bounding the Memory Backend

The memory backend is unbounded by default and holds entries for the life of the
process. Two settings bound it:

```python
cachu.configure(memory_maxsize=10_000, memory_sweep_interval=60.0)
```

- `memory_maxsize` evicts least-recently-used entries past the bound. Recency is
  tracked on reads as well as writes, which is why the entry store is an `OrderedDict` -
  roughly 25-30% more memory at 200,000 entries than a plain dict.
- `memory_sweep_interval` reclaims expired entries on an amortized schedule, so an entry
  that expires and is never read again does not stay resident until process exit.

Set `memory_maxsize` whenever the key space is influenced by callers - a credential
hash, a tenant id, a search term - since otherwise the cache grows until restart.
`memory_maxsize` defaults to the historical unbounded behaviour; the 60-second sweep is
on by default.

**Sweep cost.** A sweep is a single O(n) pass under the backend lock, so one caller per
interval pays it - and so does every other thread waiting on that lock. Measured on CPython 3.11: ~1 ms at 10,000 entries, ~20-55 ms at
200,000. If that spike matters on your hot path, set `memory_maxsize` (which caps `n`,
and therefore the sweep) or raise `memory_sweep_interval`.

`MemoryBackend` also exposes `sweep()` / `asweep()` for an immediate reclaim, and
`evictions` / `expired_swept` counters for monitoring. Reach the live instance through
the manager, matching the decorator's `package` **and** `ttl` exactly - `package`
defaults to *your* caller, not the decorator's, so omitting it silently builds a second,
empty backend whose counters stay at zero:

```python
backend = cachu.get_backend('memory', package='mylib', ttl=300)   # -1 when the decorator's ttl is callable
backend.sweep()
print(backend.evictions, backend.expired_swept)
```

Set `memory_sweep_interval=float('inf')` to disable sweeping altogether; `0` means
"sweep on every operation", which is the opposite.

Both settings are read when the backend is **constructed**, on the first cached call.
Setting them afterwards has no effect on the existing instance, so configure them at
startup (or call `cachu.clear_backends()` to force reconstruction).

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

### Scoped Disabling

`disable()` with no arguments is process-wide: a service with one optional cache and one
load-bearing cache cannot switch off the first without silently switching off the
second. Pass `package=` or `tag=` to narrow it:

```python
cachu.disable(package='myapp.docs')   # only that package's caches
cachu.disable(tag='documents')        # only caches declared with tag='documents'

cachu.enable(package='myapp.docs')    # lift one scope
cachu.enable()                        # lift the global flag and every scope
```

Scopes are OR-ed: a cache is bypassed if either its package or its tag is disabled.
`is_disabled(package, tag)` answers for a scope and `get_disabled_scopes()` returns a `DisabledScopes` snapshot with `.globally`,
`.packages` and `.tags`.

`package=` matches a cache's **resolved** package exactly - there is no prefix or
dotted-scope matching. Auto-detection resolves to the top-level name, so
`disable(package='myapp.docs')` only reaches caches declared with that exact
`package=`. A single call registers both scopes independently:
`disable(package='alpha', tag='docs')` switches off every cache in `alpha` *and* every
cache tagged `docs` anywhere - it is not the intersection. To switch off one specific
cache, use `backend='null'` on that decorator.

A scoped `enable()` cannot lift a global `disable()`; call `enable()` with no arguments
first.

For a single function, `backend='null'` is usually clearer than any disable at all, and
needs no teardown.

## Async Support

The library provides full async/await support with matching APIs. There is no separate
async decorator: `@cache` detects a coroutine function and takes the async path.

```python
from cachu import cache, async_cache_get, async_cache_set, async_cache_delete
from cachu import async_cache_clear, async_cache_info

@cache(ttl=300, backend='memory')
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

`get_redis_client(url, ...)` builds a redis-py client with cachu's resilience settings
applied. Pass your configured URL, or reuse the one cachu resolved for your package:

```python
import cachu
from cachu import get_redis_client

cfg = cachu.get_config()
client = get_redis_client(
    cfg.redis_url,
    health_check_interval=cfg.redis_health_check_interval,
    socket_timeout=cfg.redis_socket_timeout,
    retry_count=cfg.redis_retry_count,
)
client.set('direct_key', 'value')
```

To reach the client cachu is already using, go through the backend instead:

```python
backend = cachu.get_backend('redis', ttl=300)
backend.client.set('direct_key', 'value')
```

## Public API

```python
from cachu import (
    # Decorator (detects coroutine functions automatically)
    cache,

    # Configuration
    configure,
    get_config,
    get_all_configs,
    CacheConfig,
    disable,
    enable,
    is_disabled,
    get_disabled_scopes,
    DisabledScopes,

    # Sync CRUD operations
    cache_get,
    cache_set,
    cache_delete,
    cache_clear,
    cache_info,

    # Async CRUD operations
    async_cache_get,
    async_cache_set,
    async_cache_delete,
    async_cache_clear,
    async_cache_info,

    # Statistics
    get_cache_info,
    get_async_cache_info,

    # Exceptions
    CacheError,
    CacheLockTimeout,
    ConfigurationError,
    BackendNotFoundError,

    # Constants
    BACKENDS,

    # Types
    Backend,
    CacheEntry,
    CacheInfo,
    CacheMeta,

    # Advanced
    backends,
    presets,
    get_backend,
    aget_backend,
    get_redis_client,
    clear_backends,
    clear_async_backends,
)
```

## Features

- **Multiple backends**: Memory, file (SQLite), Redis, and null (passthrough)
- **Async support**: Full async/await API; `@cache` detects coroutine functions
- **Flexible TTL**: Static or dynamic TTL (callable that receives result, optionally with call args)
- **Tags**: Organize and selectively clear cache entries
- **Package isolation**: Each package gets isolated configuration, settable by name
- **Conditional caching**: Cache based on result value and/or call args
- **Args-aware predicates**: `ttl`, `cache_if`, and `validate` accept a 2-arg `(value, args)` form
- **Presets**: Composable bundles for common patterns (e.g. `today_aware` for date-keyed fetches)
- **Validation callbacks**: Validate entries before returning
- **Per-call control**: Skip or overwrite cache per call
- **Helper methods**: `.get()`, `.set()`, `.clear()`, `.refresh()`, `.original()` on decorated functions
- **Statistics**: Track hits, misses, and cache size
- **Intelligent filtering**: Auto-excludes `self`, `cls`, connections, and `_` params
- **Fail-open by default**: Backend, mutex and read faults degrade to a miss; write and stat faults are logged and never raised
- **Bounded latency**: `cache_deadline` caps cumulative cache work between operations; pair it with `redis_socket_timeout` to bound a single blocked call
- **Load shedding**: `on_lock_timeout='raise'` sheds waiters instead of stampeding
- **Bounded memory**: Optional LRU `memory_maxsize` plus amortized expiry sweeps
- **Scoped disable**: Bypass caching globally, or by package or tag
- **Cold-process clearing**: `cache_clear` reaches regions declared but not yet used
- **Redis TLS**: Supports `rediss://` URLs for secure connections
