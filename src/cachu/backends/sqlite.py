"""SQLite-based cache backend.
"""
import asyncio
import pickle
import sqlite3
import threading
import time
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any

from ..mutex import AsyncCacheMutex, AsyncioMutex, CacheMutex, ThreadingMutex
from . import NO_VALUE, Backend

if TYPE_CHECKING:
    import aiosqlite


def _get_aiosqlite_module() -> Any:
    """Import aiosqlite module, raising helpful error if not installed.
    """
    try:
        import aiosqlite
        return aiosqlite
    except ImportError as e:
        raise RuntimeError(
            "Async SQLite support requires the 'aiosqlite' package. "
            "Install with: pip install cachu[async]"
        ) from e


class SqliteBackend(Backend):
    """Unified SQLite file-based cache backend with both sync and async interfaces.
    """

    def __init__(self, filepath: str) -> None:
        self._filepath = filepath
        self._sync_lock = threading.RLock()
        self._async_lock: asyncio.Lock | None = None
        self._async_write_lock: asyncio.Lock | None = None
        self._async_connection: aiosqlite.Connection | None = None
        self._async_initialized = False
        self._init_sync_db()

    def _get_async_lock(self) -> asyncio.Lock:
        """Lazy-create async init lock (must be called from async context).
        """
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    def _get_async_write_lock(self) -> asyncio.Lock:
        """Lazy-create async write lock (must be called from async context).
        """
        if self._async_write_lock is None:
            self._async_write_lock = asyncio.Lock()
        return self._async_write_lock

    def _init_sync_db(self) -> None:
        """Initialize sync database schema.
        """
        with self._sync_lock:
            conn = sqlite3.connect(self._filepath)
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY,
                        value BLOB NOT NULL,
                        created_at REAL NOT NULL,
                        expires_at REAL NOT NULL
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cache_expires
                    ON cache(expires_at)
                """)
                conn.commit()
            finally:
                conn.close()

    async def _ensure_async_initialized(self) -> 'aiosqlite.Connection':
        """Ensure async database is initialized and return connection.
        """
        async with self._get_async_lock():
            if self._async_connection is None:
                aiosqlite = _get_aiosqlite_module()
                self._async_connection = await aiosqlite.connect(self._filepath)
                await self._async_connection.execute('PRAGMA journal_mode=WAL')
                await self._async_connection.execute('PRAGMA busy_timeout=5000')

            if not self._async_initialized:
                await self._async_connection.execute("""
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY,
                        value BLOB NOT NULL,
                        created_at REAL NOT NULL,
                        expires_at REAL NOT NULL
                    )
                """)
                await self._async_connection.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cache_expires
                    ON cache(expires_at)
                """)
                await self._async_connection.commit()
                self._async_initialized = True

        return self._async_connection

    def _get_sync_connection(self) -> sqlite3.Connection:
        """Get a sync database connection.
        """
        return sqlite3.connect(self._filepath)

    def _fnmatch_to_glob(self, pattern: str) -> str:
        """Convert fnmatch pattern to SQLite GLOB pattern.
        """
        return pattern

    def _schedule_async_delete(self, key: str) -> None:
        """Schedule a background deletion task (fire-and-forget).
        """
        async def _delete() -> None:
            try:
                async with self._get_async_write_lock():
                    conn = await self._ensure_async_initialized()
                    await conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                    await conn.commit()
            except Exception:
                pass

        asyncio.create_task(_delete())

    # ===== Sync interface =====

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found or expired.
        """
        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                cursor = conn.execute(
                    'SELECT value, expires_at FROM cache WHERE key = ?',
                    (key,),
                )
                row = cursor.fetchone()

                if row is None:
                    return NO_VALUE

                value_blob, expires_at = row
                if time.time() > expires_at:
                    conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                    conn.commit()
                    return NO_VALUE

                return pickle.loads(value_blob)
            except Exception:
                return NO_VALUE
            finally:
                conn.close()

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                cursor = conn.execute(
                    'SELECT value, created_at, expires_at FROM cache WHERE key = ?',
                    (key,),
                )
                row = cursor.fetchone()

                if row is None:
                    return NO_VALUE, None

                value_blob, created_at, expires_at = row
                if time.time() > expires_at:
                    conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                    conn.commit()
                    return NO_VALUE, None

                return pickle.loads(value_blob), created_at
            except Exception:
                return NO_VALUE, None
            finally:
                conn.close()

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """
        now = time.time()
        value_blob = pickle.dumps(value)

        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                conn.execute(
                    """INSERT OR REPLACE INTO cache (key, value, created_at, expires_at)
                       VALUES (?, ?, ?, ?)""",
                    (key, value_blob, now, now + ttl),
                )
                conn.commit()
            finally:
                conn.close()

    def delete(self, key: str) -> None:
        """Delete value by key.
        """
        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                conn.commit()
            except Exception:
                pass
            finally:
                conn.close()

    def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """
        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                if pattern is None:
                    cursor = conn.execute('SELECT COUNT(*) FROM cache')
                    count = cursor.fetchone()[0]
                    conn.execute('DELETE FROM cache')
                    conn.commit()
                    return count

                glob_pattern = self._fnmatch_to_glob(pattern)
                cursor = conn.execute(
                    'SELECT COUNT(*) FROM cache WHERE key GLOB ?',
                    (glob_pattern,),
                )
                count = cursor.fetchone()[0]
                conn.execute('DELETE FROM cache WHERE key GLOB ?', (glob_pattern,))
                conn.commit()
                return count
            except Exception:
                return 0
            finally:
                conn.close()

    def keys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys matching pattern.
        """
        now = time.time()

        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                if pattern is None:
                    cursor = conn.execute(
                        'SELECT key FROM cache WHERE expires_at > ?',
                        (now,),
                    )
                else:
                    glob_pattern = self._fnmatch_to_glob(pattern)
                    cursor = conn.execute(
                        'SELECT key FROM cache WHERE key GLOB ? AND expires_at > ?',
                        (glob_pattern, now),
                    )

                all_keys = [row[0] for row in cursor.fetchall()]
            finally:
                conn.close()

        yield from all_keys

    def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """
        now = time.time()

        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                if pattern is None:
                    cursor = conn.execute(
                        'SELECT COUNT(*) FROM cache WHERE expires_at > ?',
                        (now,),
                    )
                else:
                    glob_pattern = self._fnmatch_to_glob(pattern)
                    cursor = conn.execute(
                        'SELECT COUNT(*) FROM cache WHERE key GLOB ? AND expires_at > ?',
                        (glob_pattern, now),
                    )

                return cursor.fetchone()[0]
            except Exception:
                return 0
            finally:
                conn.close()

    def get_mutex(self, key: str) -> CacheMutex:
        """Get a mutex for dogpile prevention on the given key.
        """
        return ThreadingMutex(f'sqlite:{self._filepath}:{key}')

    def cleanup_expired(self) -> int:
        """Remove expired entries. Returns count of removed entries.
        """
        now = time.time()

        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                cursor = conn.execute(
                    'SELECT COUNT(*) FROM cache WHERE expires_at <= ?',
                    (now,),
                )
                count = cursor.fetchone()[0]
                conn.execute('DELETE FROM cache WHERE expires_at <= ?', (now,))
                conn.commit()
                return count
            finally:
                conn.close()

    # ===== Async interface =====

    async def aget(self, key: str) -> Any:
        """Async get value by key. Returns NO_VALUE if not found or expired.
        """
        try:
            conn = await self._ensure_async_initialized()
            cursor = await conn.execute(
                'SELECT value, expires_at FROM cache WHERE key = ?',
                (key,),
            )
            row = await cursor.fetchone()

            if row is None:
                return NO_VALUE

            value_blob, expires_at = row
            if time.time() > expires_at:
                self._schedule_async_delete(key)
                return NO_VALUE

            return pickle.loads(value_blob)
        except Exception:
            return NO_VALUE

    async def aget_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Async get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        try:
            conn = await self._ensure_async_initialized()
            cursor = await conn.execute(
                'SELECT value, created_at, expires_at FROM cache WHERE key = ?',
                (key,),
            )
            row = await cursor.fetchone()

            if row is None:
                return NO_VALUE, None

            value_blob, created_at, expires_at = row
            if time.time() > expires_at:
                self._schedule_async_delete(key)
                return NO_VALUE, None

            return pickle.loads(value_blob), created_at
        except Exception:
            return NO_VALUE, None

    async def aset(self, key: str, value: Any, ttl: int) -> None:
        """Async set value with TTL in seconds.
        """
        now = time.time()
        value_blob = pickle.dumps(value)

        async with self._get_async_write_lock():
            conn = await self._ensure_async_initialized()
            await conn.execute(
                """INSERT OR REPLACE INTO cache (key, value, created_at, expires_at)
                   VALUES (?, ?, ?, ?)""",
                (key, value_blob, now, now + ttl),
            )
            await conn.commit()

    async def adelete(self, key: str) -> None:
        """Async delete value by key.
        """
        async with self._get_async_write_lock():
            try:
                conn = await self._ensure_async_initialized()
                await conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                await conn.commit()
            except Exception:
                pass

    async def aclear(self, pattern: str | None = None) -> int:
        """Async clear entries matching pattern. Returns count of cleared entries.
        """
        async with self._get_async_write_lock():
            try:
                conn = await self._ensure_async_initialized()
                if pattern is None:
                    cursor = await conn.execute('SELECT COUNT(*) FROM cache')
                    row = await cursor.fetchone()
                    count = row[0]
                    await conn.execute('DELETE FROM cache')
                    await conn.commit()
                    return count

                glob_pattern = self._fnmatch_to_glob(pattern)
                cursor = await conn.execute(
                    'SELECT COUNT(*) FROM cache WHERE key GLOB ?',
                    (glob_pattern,),
                )
                row = await cursor.fetchone()
                count = row[0]
                await conn.execute('DELETE FROM cache WHERE key GLOB ?', (glob_pattern,))
                await conn.commit()
                return count
            except Exception:
                return 0

    async def akeys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Async iterate over keys matching pattern.
        """
        now = time.time()
        conn = await self._ensure_async_initialized()

        if pattern is None:
            cursor = await conn.execute(
                'SELECT key FROM cache WHERE expires_at > ?',
                (now,),
            )
        else:
            glob_pattern = self._fnmatch_to_glob(pattern)
            cursor = await conn.execute(
                'SELECT key FROM cache WHERE key GLOB ? AND expires_at > ?',
                (glob_pattern, now),
            )

        all_keys = [row[0] for row in await cursor.fetchall()]

        for key in all_keys:
            yield key

    async def acount(self, pattern: str | None = None) -> int:
        """Async count keys matching pattern.
        """
        now = time.time()

        try:
            conn = await self._ensure_async_initialized()
            if pattern is None:
                cursor = await conn.execute(
                    'SELECT COUNT(*) FROM cache WHERE expires_at > ?',
                    (now,),
                )
            else:
                glob_pattern = self._fnmatch_to_glob(pattern)
                cursor = await conn.execute(
                    'SELECT COUNT(*) FROM cache WHERE key GLOB ? AND expires_at > ?',
                    (glob_pattern, now),
                )

            row = await cursor.fetchone()
            return row[0]
        except Exception:
            return 0

    def get_async_mutex(self, key: str) -> AsyncCacheMutex:
        """Get an async mutex for dogpile prevention on the given key.
        """
        return AsyncioMutex(f'sqlite:{self._filepath}:{key}')

    async def acleanup_expired(self) -> int:
        """Async remove expired entries. Returns count of removed entries.
        """
        now = time.time()

        async with self._get_async_write_lock():
            conn = await self._ensure_async_initialized()
            cursor = await conn.execute(
                'SELECT COUNT(*) FROM cache WHERE expires_at <= ?',
                (now,),
            )
            row = await cursor.fetchone()
            count = row[0]
            await conn.execute('DELETE FROM cache WHERE expires_at <= ?', (now,))
            await conn.commit()
            return count

    # ===== Lifecycle =====

    def _close_async_connection_sync(self) -> None:
        """Forcefully close async connection from sync context.

        This accesses aiosqlite internals as there's no public sync close API.
        """
        if self._async_connection is None:
            return

        conn = self._async_connection
        self._async_connection = None
        self._async_initialized = False

        try:
            conn._running = False
            if hasattr(conn, '_connection') and conn._connection:
                conn._connection.close()
        except Exception:
            pass

    def close(self) -> None:
        """Close all backend resources from sync context.
        """
        self._close_async_connection_sync()

    async def aclose(self) -> None:
        """Close all backend resources from async context.
        """
        if self._async_connection is not None:
            conn = self._async_connection
            self._async_connection = None
            self._async_initialized = False
            await conn.close()
