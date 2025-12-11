"""File-based cache backend using DBM.
"""
import dbm
import fnmatch
import pathlib
import pickle
import struct
import threading
import time
from collections.abc import Iterator
from typing import Any

from . import NO_VALUE, Backend

_METADATA_FORMAT = 'dd'
_METADATA_SIZE = struct.calcsize(_METADATA_FORMAT)


class FileBackend(Backend):
    """DBM file-based cache backend.
    """

    def __init__(self, filepath: str) -> None:
        self._filepath = filepath
        self._lock = threading.RLock()
        self._ensure_dir()

    def _ensure_dir(self) -> None:
        """Ensure the directory for the cache file exists.
        """
        directory = pathlib.Path(self._filepath).parent
        if directory and not pathlib.Path(directory).exists():
            pathlib.Path(directory).mkdir(exist_ok=True, parents=True)

    def _pack_value(self, value: Any, created_at: float, expires_at: float) -> bytes:
        """Pack value with metadata.
        """
        metadata = struct.pack(_METADATA_FORMAT, created_at, expires_at)
        pickled = pickle.dumps(value)
        return metadata + pickled

    def _unpack_value(self, data: bytes) -> tuple[Any, float, float]:
        """Unpack value and metadata.
        """
        created_at, expires_at = struct.unpack(_METADATA_FORMAT, data[:_METADATA_SIZE])
        value = pickle.loads(data[_METADATA_SIZE:])
        return value, created_at, expires_at

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found or expired.
        """
        with self._lock:
            try:
                with dbm.open(self._filepath, 'c') as db:
                    data = db.get(key.encode())
                    if data is None:
                        return NO_VALUE

                    value, created_at, expires_at = self._unpack_value(data)
                    if time.time() > expires_at:
                        del db[key.encode()]
                        return NO_VALUE

                    return value
            except Exception:
                return NO_VALUE

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        with self._lock:
            try:
                with dbm.open(self._filepath, 'c') as db:
                    data = db.get(key.encode())
                    if data is None:
                        return NO_VALUE, None

                    value, created_at, expires_at = self._unpack_value(data)
                    if time.time() > expires_at:
                        del db[key.encode()]
                        return NO_VALUE, None

                    return value, created_at
            except Exception:
                return NO_VALUE, None

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """
        now = time.time()
        packed = self._pack_value(value, now, now + ttl)
        with self._lock, dbm.open(self._filepath, 'c') as db:
            db[key.encode()] = packed

    def delete(self, key: str) -> None:
        """Delete value by key.
        """
        with self._lock:
            try:
                with dbm.open(self._filepath, 'c') as db:
                    if key.encode() in db:
                        del db[key.encode()]
            except Exception:
                pass

    def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """
        with self._lock:
            try:
                if pattern is None:
                    with dbm.open(self._filepath, 'n'):
                        pass
                    return -1

                with dbm.open(self._filepath, 'c') as db:
                    keys_to_delete = [
                        k for k in db
                        if fnmatch.fnmatch(k.decode(), pattern)
                    ]
                    for key in keys_to_delete:
                        del db[key]
                    return len(keys_to_delete)
            except Exception:
                return 0

    def keys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys matching pattern.
        """
        now = time.time()
        with self._lock:
            try:
                with dbm.open(self._filepath, 'c') as db:
                    all_keys = [k.decode() for k in db]
            except Exception:
                return

        for key in all_keys:
            with self._lock:
                try:
                    with dbm.open(self._filepath, 'c') as db:
                        data = db.get(key.encode())
                        if data is None:
                            continue
                        _, _, expires_at = self._unpack_value(data)
                        if now > expires_at:
                            del db[key.encode()]
                            continue
                except Exception:
                    continue

            if pattern is None or fnmatch.fnmatch(key, pattern):
                yield key

    def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """
        return sum(1 for _ in self.keys(pattern))
