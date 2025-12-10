"""Test exclude_params functionality for cache decorators.
"""
import cache
import pytest


def test_exclude_params_basic_memory():
    """Verify exclude_params excludes specified parameters from cache key.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(exclude_params={'logger', 'context'})
    def process_data(logger, context, user_id: int, data: str) -> dict:
        nonlocal call_count
        call_count += 1
        return {'user_id': user_id, 'data': data, 'logger': str(logger), 'context': str(context)}

    result1 = process_data('logger1', 'ctx1', 123, 'test')
    result2 = process_data('logger2', 'ctx2', 123, 'test')

    assert result1['user_id'] == 123
    assert result1['data'] == 'test'
    assert result1['logger'] == 'logger1'
    assert result2['logger'] == 'logger1'
    assert result2['context'] == 'ctx1'
    assert call_count == 1


def test_exclude_params_single_param():
    """Verify exclude_params works with a single excluded parameter.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(exclude_params={'timestamp'})
    def fetch_data(timestamp: str, user_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'timestamp': timestamp, 'user_id': user_id}

    fetch_data('2024-01-01', 123)
    fetch_data('2024-01-02', 123)
    fetch_data('2024-01-03', 123)

    assert call_count == 1

    fetch_data('2024-01-04', 456)

    assert call_count == 2


def test_exclude_params_multiple_params():
    """Verify exclude_params works with multiple excluded parameters.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(exclude_params={'logger', 'debug', 'verbose'})
    def complex_func(logger, debug: bool, verbose: bool, user_id: int, action: str) -> dict:
        nonlocal call_count
        call_count += 1
        return {'user_id': user_id, 'action': action}

    complex_func('log1', True, False, 123, 'create')
    complex_func('log2', False, True, 123, 'create')
    complex_func('log3', True, True, 123, 'create')

    assert call_count == 1

    complex_func('log4', False, False, 123, 'update')

    assert call_count == 2


def test_exclude_params_with_namespace():
    """Verify exclude_params works correctly with namespaces.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(namespace='users', exclude_params={'session_id'})
    def get_user(session_id: str, user_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'user_id': user_id}

    get_user('session1', 123)
    get_user('session2', 123)

    assert call_count == 1


def test_exclude_params_with_defaults():
    """Verify exclude_params works with default parameter values.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(exclude_params={'debug'})
    def fetch_data(user_id: int, debug: bool = False) -> dict:
        nonlocal call_count
        call_count += 1
        return {'user_id': user_id}

    fetch_data(123, debug=True)
    fetch_data(123, debug=False)
    fetch_data(123)

    assert call_count == 1


def test_exclude_params_with_kwargs():
    """Verify exclude_params works with keyword arguments.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(exclude_params={'meta'})
    def process(user_id: int, data: str, meta: dict = None) -> dict:
        nonlocal call_count
        call_count += 1
        return {'user_id': user_id, 'data': data}

    process(123, 'test', meta={'version': 1})
    process(123, 'test', meta={'version': 2})
    process(user_id=123, data='test', meta={'version': 3})

    assert call_count == 1


def test_exclude_params_combined_with_connection_filtering():
    """Verify exclude_params works with connection object filtering.
    """
    call_count = 0

    class MockConnection:
        def __init__(self):
            self.driver_connection = True

    @cache.memorycache(seconds=300).cache_on_arguments(exclude_params={'logger'})
    def query_data(conn, logger, user_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'user_id': user_id}

    conn1 = MockConnection()
    conn2 = MockConnection()

    query_data(conn1, 'log1', 123)
    query_data(conn2, 'log2', 123)

    assert call_count == 1


def test_exclude_params_combined_with_underscore_filtering():
    """Verify exclude_params works alongside underscore parameter filtering.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(exclude_params={'debug'})
    def process_data(_internal: str, debug: bool, user_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'user_id': user_id}

    process_data('internal1', True, 123)
    process_data('internal2', False, 123)

    assert call_count == 1


def test_exclude_params_empty_set():
    """Verify empty exclude_params set has no effect.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(exclude_params=set())
    def func(x: int, y: int) -> int:
        nonlocal call_count
        call_count += 1
        return x + y

    func(5, 10)
    func(5, 10)

    assert call_count == 1

    func(5, 20)

    assert call_count == 2


@pytest.mark.parametrize(('cache_func', 'fixture_needed'), [
    (lambda: cache.filecache(seconds=300), 'temp_cache_dir'),
    (lambda: cache.rediscache(seconds=300), 'redis_docker'),
])
def test_exclude_params_backend_single(cache_func, fixture_needed, request):
    """Verify exclude_params works with different cache backends.
    """
    if fixture_needed == 'redis_docker':
        pytest.importorskip('redis')
        request.getfixturevalue(fixture_needed)
    elif fixture_needed == 'temp_cache_dir':
        request.getfixturevalue(fixture_needed)

    call_count = 0

    @cache_func().cache_on_arguments(exclude_params={'logger'})
    def func(logger, item_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'item_id': item_id}

    func('log1', 100)
    func('log2', 100)
    func('log3', 100)

    assert call_count == 1

    func('log4', 200)

    assert call_count == 2


@pytest.mark.parametrize(('cache_func', 'fixture_needed'), [
    (lambda: cache.filecache(seconds=300), 'temp_cache_dir'),
    (lambda: cache.rediscache(seconds=300), 'redis_docker'),
])
def test_exclude_params_backend_multiple(cache_func, fixture_needed, request):
    """Verify exclude_params works with multiple parameters in different backends.
    """
    if fixture_needed == 'redis_docker':
        pytest.importorskip('redis')
        request.getfixturevalue(fixture_needed)
    elif fixture_needed == 'temp_cache_dir':
        request.getfixturevalue(fixture_needed)

    call_count = 0

    @cache_func().cache_on_arguments(exclude_params={'param1', 'param2'})
    def func(param1: str, param2: str, user_id: int, key: str) -> dict:
        nonlocal call_count
        call_count += 1
        return {'user_id': user_id, 'key': key}

    func('val1', 'val2', 123, 'test')
    func('val3', 'val4', 123, 'test')

    assert call_count == 1


def test_exclude_params_preserves_included_param_uniqueness():
    """Verify excluded parameters don't affect cache key uniqueness based on included parameters.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(exclude_params={'ignored'})
    def func(ignored: str, x: int, y: int) -> int:
        nonlocal call_count
        call_count += 1
        return x + y

    func('a', 1, 2)
    func('b', 1, 2)
    assert call_count == 1

    func('c', 1, 3)
    assert call_count == 2

    func('d', 2, 2)
    assert call_count == 3


def test_exclude_params_with_instance_method():
    """Verify exclude_params works with instance methods.
    """
    class Service:
        def __init__(self):
            self.call_count = 0

        @cache.memorycache(seconds=300).cache_on_arguments(exclude_params={'logger'})
        def get_data(self, logger, user_id: int) -> dict:
            self.call_count += 1
            return {'user_id': user_id}

    service = Service()

    service.get_data('log1', 123)
    service.get_data('log2', 123)
    service.get_data('log3', 123)

    assert service.call_count == 1


def test_exclude_params_clearing_namespace():
    """Verify clearing cache by namespace works with exclude_params.
    """
    @cache.memorycache(seconds=300).cache_on_arguments(namespace='data', exclude_params={'version'})
    def get_data(version: int, key: str) -> dict:
        return {'key': key, 'value': 'data'}

    get_data(1, 'key1')
    get_data(2, 'key1')

    from conftest import get_memory_region
    cache_dict = get_memory_region(300).actual_backend._cache
    assert len(cache_dict) == 1

    cache.clear_memorycache(seconds=300, namespace='data')

    assert len(cache_dict) == 0


def test_exclude_params_set_key():
    """Verify set_memorycache_key works with exclude_params decorated functions.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(namespace='users', exclude_params={'logger'})
    def get_user(logger, user_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'id': user_id, 'name': f'user_{user_id}'}

    get_user('log1', 123)
    assert call_count == 1

    cache.set_memorycache_key(300, 'users', get_user, {'id': 123, 'name': 'updated_user'}, user_id=123)

    result = get_user('log2', 123)
    assert result['name'] == 'updated_user'
    assert call_count == 1


def test_exclude_params_delete_key():
    """Verify delete_memorycache_key works with exclude_params decorated functions.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(namespace='users', exclude_params={'session'})
    def get_user(session: str, user_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'id': user_id}

    get_user('sess1', 123)
    get_user('sess2', 123)
    assert call_count == 1

    cache.delete_memorycache_key(300, 'users', get_user, user_id=123)

    get_user('sess3', 123)
    assert call_count == 2


def test_exclude_params_use_case_from_requirements():
    """Verify the original use case with logger and context parameters.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments(exclude_params={'logger', 'context'})
    def calculate(logger, context, x: int, y: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * y

    calculate('log1', {'env': 'dev'}, 5, 10)
    calculate('log2', {'env': 'prod'}, 5, 10)
    calculate('log3', {'env': 'test'}, 5, 10)

    assert call_count == 1

    calculate('log4', {'env': 'staging'}, 5, 20)

    assert call_count == 2
