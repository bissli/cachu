"""DynamoDB Local (testcontainers) fixtures for cache tests.
"""
import logging
import time

import cachu
import pytest
from cachu.backends.dynamodb import create_dynamodb_table

logger = logging.getLogger(__name__)

TABLE_NAME = 'cachu-cache'
_READY_DEADLINE = 60.0


@pytest.fixture(scope='session')
def dynamodb_docker():
    """Start a DynamoDB Local container and yield its endpoint URL.

    Notes
    -----
    - One container serves the whole session, exactly as `redis_docker`
      does; per-test isolation comes from `dynamodb_table` dropping
      every table on teardown.
    - The image tag is pinned: the engine's TTL sweep cadence and
      request validation strictness are part of what the suite encodes,
      and a floating tag would change them silently.
    - '-inMemory' keeps the store off disk; '-sharedDb' collapses the
      engine's per-credential-and-region database namespacing into one
      database, so every client sees the same tables no matter which
      fake credentials built it.
    - Unlike real AWS's days-lazy collection, the engine really deletes
      expired rows on a roughly 10-second TTL sweep: a test that needs
      an expired row to survive must keep it invisible to the sweeper
      (omit 'expires_ttl', or use a table with TTL never enabled).
    - The log wait only proves the process started - the banner is
      printed before the HTTP listener binds - so readiness is probed
      with a no-retry ListTables loop before yielding; without it the
      first test's setup survives on botocore's default retry budget
      alone.
    """
    # Deferred imports: conftest loads this module via pytest_plugins,
    # so top-level testcontainers/boto3 imports would break collection
    # of the whole suite on an environment without the test extra.
    import boto3
    from botocore.config import Config
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    container = DockerContainer('amazon/dynamodb-local:3.3.1')
    container.with_command(
        '-jar DynamoDBLocal.jar -inMemory -sharedDb -disableTelemetry')
    container.with_exposed_ports(8000)
    container.waiting_for(LogMessageWaitStrategy('Initializing DynamoDB Local'))
    with container as running:
        host = running.get_container_host_ip()
        port = running.get_exposed_port(8000)
        endpoint = f'http://{host}:{port}'

        probe = boto3.session.Session().client(
            'dynamodb',
            endpoint_url=endpoint,
            region_name='us-east-1',
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            config=Config(
                retries={'total_max_attempts': 1},
                connect_timeout=2,
                read_timeout=2))
        started = time.monotonic()
        deadline = started + _READY_DEADLINE
        while True:
            try:
                probe.list_tables()
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        probe.close()
        logger.debug(
            f'DynamoDB Local ready at {endpoint} after '
            f'{time.monotonic() - started:.2f}s')
        yield endpoint


@pytest.fixture
def dynamodb_table(monkeypatch, dynamodb_docker):
    """Run the test against DynamoDB Local with the cache table created.

    Notes
    -----
    - The table name matches `CacheConfig.dynamodb_table`'s default, so
      decorator-driven tests need no extra configuration.
    - AWS_ENDPOINT_URL_DYNAMODB points every client built without an
      explicit endpoint at the container: the backend and the table
      helper pass endpoint_url=None straight through to botocore, whose
      resolution chain consults the env var next (botocore >= 1.31,
      hence the test extra's boto3 >= 1.28 floor).
    - The AWS env is made hermetic: static fake credentials and
      neutered profile/config/retry/endpoint-override sources mean any
      client that escaped the endpoint routing fails auth instead of
      reaching real AWS, and a developer's retry env cannot reshape the
      suite's request behavior.
    - Teardown closes manager-held backends first - close() flushes
      buffered stats, which must land in the container while the
      endpoint env is still patched - then drops every table, so one
      test's rows, locks and stats never leak into the next.
    """
    # Deferred import: same collection concern as in dynamodb_docker.
    import boto3

    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_SECURITY_TOKEN', 'testing')
    monkeypatch.setenv('AWS_SESSION_TOKEN', 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    monkeypatch.delenv('AWS_PROFILE', raising=False)
    monkeypatch.setenv('AWS_CONFIG_FILE', '/dev/null')
    monkeypatch.setenv('AWS_SHARED_CREDENTIALS_FILE', '/dev/null')
    monkeypatch.setenv('AWS_ENDPOINT_URL_DYNAMODB', dynamodb_docker)
    monkeypatch.delenv('AWS_IGNORE_CONFIGURED_ENDPOINT_URLS', raising=False)
    monkeypatch.delenv('AWS_MAX_ATTEMPTS', raising=False)
    monkeypatch.delenv('AWS_RETRY_MODE', raising=False)

    create_dynamodb_table(TABLE_NAME)
    yield TABLE_NAME

    cachu.clear_backends()
    client = boto3.session.Session().client(
        'dynamodb', endpoint_url=dynamodb_docker)
    for table_name in client.list_tables()['TableNames']:
        client.delete_table(TableName=table_name)
    client.close()
