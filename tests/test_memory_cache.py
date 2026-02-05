"""Test memory cache backend operations via inheritance-based test suite.
"""
import pytest
from cachu.backends.memory import MemoryBackend
from fixtures.backend_suite import _GenericAsyncBackendTestSuite
from fixtures.backend_suite import _GenericAsyncDirectBackendTestSuite
from fixtures.backend_suite import _GenericBackendTestSuiteWithTTL
from fixtures.backend_suite import _GenericDirectBackendTestSuite


class TestMemoryBackend(_GenericBackendTestSuiteWithTTL):
    """Sync tests for memory backend via decorator.
    """

    backend = 'memory'


class TestAsyncMemoryBackend(_GenericAsyncBackendTestSuite):
    """Async tests for memory backend via decorator.
    """

    backend = 'memory'


class TestMemoryBackendDirect(_GenericDirectBackendTestSuite):
    """Direct API tests for MemoryBackend.
    """

    def create_backend(self):
        """Create MemoryBackend instance.
        """
        return MemoryBackend()


@pytest.mark.asyncio
class TestAsyncMemoryBackendDirect(_GenericAsyncDirectBackendTestSuite):
    """Async direct API tests for MemoryBackend.
    """

    def create_backend(self):
        """Create MemoryBackend instance.
        """
        return MemoryBackend()
