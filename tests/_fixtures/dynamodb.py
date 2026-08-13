"""Moto-backed DynamoDB fixture for cache tests.
"""
import logging

import pytest
from cachu.backends.dynamodb import create_dynamodb_table

logger = logging.getLogger(__name__)

TABLE_NAME = 'cachu-cache'


@pytest.fixture
def dynamodb_mock(monkeypatch):
    """Run the test against an in-process moto DynamoDB with the table created.

    Notes
    -----
    - The table name matches `CacheConfig.dynamodb_table`'s default, so
      decorator-driven tests need no extra configuration.
    - moto intercepts any client created while the context is active;
      cachu builds its client lazily on first backend operation, which is
      always inside the test body.
    - The AWS env is made hermetic: fake credentials so no real chain is
      consulted, and profile/config sources removed so a developer's
      AWS_PROFILE cannot break Session construction, which happens before
      moto could intercept anything.
    """
    # Deferred import: conftest loads this module via pytest_plugins, so a
    # top-level moto import would break collection of the whole suite on
    # an environment without the test extra installed.
    from moto import mock_aws

    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_SECURITY_TOKEN', 'testing')
    monkeypatch.setenv('AWS_SESSION_TOKEN', 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    monkeypatch.delenv('AWS_PROFILE', raising=False)
    monkeypatch.setenv('AWS_CONFIG_FILE', '/dev/null')
    monkeypatch.setenv('AWS_SHARED_CREDENTIALS_FILE', '/dev/null')

    with mock_aws():
        create_dynamodb_table(TABLE_NAME)
        yield TABLE_NAME
        # Close manager-held backends INSIDE the mock: close() flushes
        # buffered stats, and conftest's own teardown runs after moto has
        # unpatched botocore, where that flush would hit real AWS.
        import cachu
        cachu.clear_backends()
