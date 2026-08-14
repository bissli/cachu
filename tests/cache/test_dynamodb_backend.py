"""Test DynamoDB cache backend operations via inheritance-based test suite.
"""
import pytest
from _fixtures.backend_suite import _GenericAsyncBackendTestSuite
from _fixtures.backend_suite import _GenericAsyncDirectBackendTestSuite
from _fixtures.backend_suite import _GenericBackendTestSuiteWithTTL
from _fixtures.backend_suite import _GenericDirectBackendTestSuite
from _fixtures.dynamodb import TABLE_NAME
from cachu.backends.dynamodb import DynamoDBBackend


class TestDynamoDBBackend(_GenericBackendTestSuiteWithTTL):
    """Sync tests for DynamoDB backend via decorator.
    """

    backend = 'dynamodb'

    @pytest.fixture(autouse=True)
    def setup_dynamodb(self, dynamodb_table):
        """Ensure the DynamoDB Local table exists.
        """


class TestAsyncDynamoDBBackend(_GenericAsyncBackendTestSuite):
    """Async tests for DynamoDB backend via decorator.
    """

    backend = 'dynamodb'

    @pytest.fixture(autouse=True)
    def setup_dynamodb(self, dynamodb_table):
        """Ensure the DynamoDB Local table exists.
        """


class TestDynamoDBBackendDirect(_GenericDirectBackendTestSuite):
    """Direct API tests for DynamoDBBackend.
    """

    @pytest.fixture(autouse=True)
    def setup_dynamodb(self, dynamodb_table):
        """Ensure the DynamoDB Local table exists.
        """

    def create_backend(self):
        """Create DynamoDBBackend instance.
        """
        return DynamoDBBackend(TABLE_NAME)


@pytest.mark.asyncio
class TestAsyncDynamoDBBackendDirect(_GenericAsyncDirectBackendTestSuite):
    """Async direct API tests for DynamoDBBackend.
    """

    @pytest.fixture(autouse=True)
    def setup_dynamodb(self, dynamodb_table):
        """Ensure the DynamoDB Local table exists.
        """

    def create_backend(self):
        """Create DynamoDBBackend instance.
        """
        return DynamoDBBackend(TABLE_NAME)
