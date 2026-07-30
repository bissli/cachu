"""Tests that one sync Redis connect attempt shares one socket budget.

Notes
-----
- redis-py's SYNC `Connection._connect` loops over every address
  `getaddrinfo` returns and applies `socket_connect_timeout` to EACH of
  them, so an endpoint resolving to n A records - an ElastiCache serverless
  endpoint does - costs `redis_socket_timeout * n * (1 + retry_count)`
  against a blackhole, not the `redis_socket_timeout * (1 + retry_count)`
  cachu documents and warns about.
- redis-py's ASYNC `_connect` already wraps the whole address loop in a
  single `async_timeout(socket_connect_timeout)`, so the guarantee existed
  on one path and not the other.
- The ceiling is deliberately not flat. Each address keeps a guaranteed
  `_MIN_CONNECT_FRACTION` of the budget, so one attempt costs at most
  `budget * (1 + (n - 1) * _MIN_CONNECT_FRACTION)`. Without that share,
  bounding the total silently destroyed redis-py's per-address failover:
  every address after the first got a millisecond, which no real network
  hop completes in.
- These tests fake the resolver and the socket rather than a network fault,
  so the assertions are on the timeouts redis-py actually applied - a wall
  clock alone would be flaky and would not say WHY it was slow.
"""
import socket
import threading
import time

import pytest
import redis
from cachu.backends.redis import _MIN_CONNECT_FRACTION, _connect_budget_class
from cachu.backends.redis import get_redis_client

BUDGET = 0.25
ADDRESS_COUNT = 3
FAKE_HOST = 'blackhole.cachu-test.invalid'

PER_ADDRESS_FLOOR = BUDGET * _MIN_CONNECT_FRACTION
ATTEMPT_CEILING = BUDGET * (1 + (ADDRESS_COUNT - 1) * _MIN_CONNECT_FRACTION)


class _Blackhole:
    """A resolver and socket pair that swallows connects to a fake host.

    Attributes
    ----------
    timeouts : list of float
        The `socket_connect_timeout` redis-py applied to each connect
        attempt, in order. This is the oracle: its SUM is the attempt cost
        and its MINIMUM is the per-address guarantee.
    """

    def __init__(self) -> None:
        self.timeouts: list[float] = []


@pytest.fixture
def blackhole(monkeypatch):
    """Resolve the fake host to several addresses that all time out.
    """
    state = _Blackhole()
    real_socket = socket.socket
    real_getaddrinfo = socket.getaddrinfo

    def fake_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        if host != FAKE_HOST:
            return real_getaddrinfo(host, port, family, type, proto, flags)
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', (f'10.255.255.{i}', port))
            for i in range(1, ADDRESS_COUNT + 1)
        ]

    class FakeSocket(real_socket):
        def connect(self, address):
            if not str(address[0]).startswith('10.255.255.'):
                return super().connect(address)
            timeout = self.gettimeout()
            state.timeouts.append(timeout)
            time.sleep(timeout)
            raise socket.timeout('timed out')

    monkeypatch.setattr(socket, 'getaddrinfo', fake_getaddrinfo)
    monkeypatch.setattr(socket, 'socket', FakeSocket)
    return state


@pytest.fixture
def dead_then_healthy(monkeypatch):
    """Resolve to a blackholed address followed by a reachable one.

    The healthy address answers after a delay well above the absolute
    `_MIN_CONNECT_SLICE`, standing in for any real network hop: a
    cross-AZ connect is 0.5-2 ms and a loopback one is faster than any
    floor, so a loopback-speed peer could not tell a starved slice from a
    working one.
    """
    healthy_rtt = 0.005
    real_socket = socket.socket
    real_getaddrinfo = socket.getaddrinfo

    listener = real_socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(('127.0.0.1', 0))
    listener.listen(64)
    port = listener.getsockname()[1]
    accepting = threading.Thread(
        target=lambda: [listener.accept() for _ in range(64)], daemon=True)
    accepting.start()

    def fake_getaddrinfo(host, p, family=0, type=0, proto=0, flags=0):
        if host != FAKE_HOST:
            return real_getaddrinfo(host, p, family, type, proto, flags)
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('10.255.255.1', p)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', port)),
        ]

    class FakeSocket(real_socket):
        def connect(self, address):
            timeout = self.gettimeout()
            if str(address[0]) == '10.255.255.1':
                time.sleep(timeout)
                raise socket.timeout('timed out')
            if timeout is not None and healthy_rtt > timeout:
                time.sleep(timeout)
                raise socket.timeout('timed out')
            time.sleep(healthy_rtt)
            return super().connect(address)

    monkeypatch.setattr(socket, 'getaddrinfo', fake_getaddrinfo)
    monkeypatch.setattr(socket, 'socket', FakeSocket)
    yield
    listener.close()


def test_a_healthy_address_behind_a_dead_one_stays_reachable(dead_then_healthy):
    """Bounding the budget must not destroy redis-py's per-address failover.

    Mutation: floor the per-address timeout at the absolute
    `_MIN_CONNECT_SLICE` instead of a share of the budget. The first address
    then burns the whole budget and every later address gets 1 ms - shorter
    than any real network hop - so one bad address in a multi-address
    endpoint turns a slow cache into an unavailable one, and `fail_open`
    quietly sends every call to the origin. Every retry round restarts at
    the same dead address, so retries cannot rescue it.
    Oracle: plain `redis.Connection` on the identical fake resolver, which
    reaches the healthy address. A differential against redis-py's own
    failover, not a restatement of ours.
    """
    plain = redis.Connection(
        host=FAKE_HOST, port=6379,
        socket_connect_timeout=BUDGET, socket_timeout=BUDGET)
    plain_sock = plain._connect()
    plain_sock.close()

    budgeted = _connect_budget_class(redis.Connection)(
        host=FAKE_HOST, port=6379,
        socket_connect_timeout=BUDGET, socket_timeout=BUDGET)
    budgeted_sock = budgeted._connect()
    budgeted_sock.close()


def test_one_operation_shares_one_budget_across_every_address(blackhole):
    """A multi-address endpoint costs far less than n budgets.

    Mutation: stop overriding the connect timeout, restoring redis-py's
    per-address application - every address gets a full budget and the
    single operation costs 3x what `redis_socket_timeout` implies.
    Oracle: the timeouts redis-py actually applied. All 3 addresses are
    tried, and their sum fits the documented
    `budget * (1 + (n - 1) * _MIN_CONNECT_FRACTION)` ceiling, which is well
    under the 3 budgets redis-py would spend.
    """
    client = get_redis_client(
        f'redis://{FAKE_HOST}:6379/0', socket_timeout=BUDGET, retry_count=0)

    started = time.monotonic()
    with pytest.raises(Exception):
        client.get('probe')
    elapsed = time.monotonic() - started

    assert len(blackhole.timeouts) == ADDRESS_COUNT
    assert sum(blackhole.timeouts) <= ATTEMPT_CEILING
    assert sum(blackhole.timeouts) < BUDGET * ADDRESS_COUNT
    assert elapsed < BUDGET * ADDRESS_COUNT


def test_retry_count_multiplies_the_attempt_and_nothing_else(blackhole):
    """Each retry round costs one attempt ceiling, not one per address.

    Mutation: any change that reintroduces a per-address multiplier makes
    the sum 6 budgets rather than 2 ceilings, which is the arithmetic
    `_warn_if_deadline_unenforceable` reports to the operator.
    Oracle: 2 retry rounds x 3 addresses = 6 attempts whose timeouts sum to
    at most 2 attempt ceilings.
    """
    client = get_redis_client(
        f'redis://{FAKE_HOST}:6379/0', socket_timeout=BUDGET, retry_count=1)

    with pytest.raises(Exception):
        client.get('probe')

    assert len(blackhole.timeouts) == ADDRESS_COUNT * 2
    assert sum(blackhole.timeouts) <= ATTEMPT_CEILING * 2


def test_every_address_is_guaranteed_a_usable_share_of_the_budget(blackhole):
    """A starved address gets a share of the budget, not a token slice.

    Mutation: return the raw remaining budget, or floor it at the absolute
    `_MIN_CONNECT_SLICE`. Raw goes non-positive, and `settimeout(0)` makes
    the socket NON-BLOCKING while a negative value raises ValueError; the
    1 ms absolute floor is positive but too short to complete a real
    connect, which is what silently breaks failover.
    Oracle: the applied timeouts, each of which must reach the documented
    per-address floor - a bound `all(t > 0)` would not have caught.
    """
    client = get_redis_client(
        f'redis://{FAKE_HOST}:6379/0', socket_timeout=BUDGET, retry_count=0)

    with pytest.raises(Exception):
        client.get('probe')

    assert blackhole.timeouts
    assert all(timeout >= PER_ADDRESS_FLOOR for timeout in blackhole.timeouts)


def test_the_budget_is_released_after_a_real_connect_attempt(blackhole):
    """A finished connect leaves the full budget for the next attempt.

    Mutation: drop the `finally` that clears `_connect_deadline`. The
    connection is then permanently clamped to the per-address floor, so
    every later reconnect - the pool makes one whenever a connection is
    dropped - starts already spent.
    Oracle: the property's value after a real `_connect` has run and
    failed, which must be the configured budget again.
    """
    connection = _connect_budget_class(redis.Connection)(
        host=FAKE_HOST, port=6379,
        socket_connect_timeout=BUDGET, socket_timeout=BUDGET)

    assert connection.socket_connect_timeout == BUDGET

    with pytest.raises(Exception):
        connection._connect()

    assert connection._connect_deadline is None
    assert connection.socket_connect_timeout == BUDGET


def test_a_spent_budget_never_yields_a_non_blocking_socket():
    """The floor holds even for a budget already in the past.

    Mutation: return the raw remaining budget. `settimeout(0)` is
    NON-BLOCKING and a negative value raises ValueError out of connect.
    Oracle: the property's value with the deadline five seconds gone.
    """
    connection = _connect_budget_class(redis.Connection)(
        host=FAKE_HOST, port=6379, socket_connect_timeout=BUDGET)

    connection._connect_deadline = time.monotonic() - 5
    assert connection.socket_connect_timeout >= PER_ADDRESS_FLOOR


def test_an_unset_timeout_is_left_alone():
    """Without a configured timeout there is no budget to enforce.

    Mutation: substitute a floor for None, which silently imposes a
    millisecond connect timeout on a caller who deliberately configured
    none.
    Oracle: the property's value, None.
    """
    connection = _connect_budget_class(redis.Connection)(
        host=FAKE_HOST, port=6379, socket_connect_timeout=None, socket_timeout=None)

    assert connection.socket_connect_timeout is None
    connection._connect_deadline = time.monotonic() - 5
    assert connection.socket_connect_timeout is None


@pytest.mark.redis
def test_the_budgeted_connection_still_talks_to_a_live_server(redis_docker):
    """Bounding the connect must not break a working connection.

    Mutation: an override that returns the floor unconditionally, or one
    that breaks the inherited setter, leaves the client unable to connect at
    all - a bound of zero satisfies every timing assertion above.
    Oracle: a round trip through a live server, read back with a
    separately-created client.
    """
    from _fixtures.redis import redis_test_config

    url = f'redis://{redis_test_config.host}:{redis_test_config.port}/0'
    client = get_redis_client(url, socket_timeout=BUDGET, retry_count=0)
    try:
        client.set('cachu-connect-budget', b'alive')
        assert client.get('cachu-connect-budget') == b'alive'
    finally:
        client.delete('cachu-connect-budget')
        client.close()

    reader = get_redis_client(url, socket_timeout=BUDGET, retry_count=0)
    try:
        assert reader.ping() is True
    finally:
        reader.close()


@pytest.mark.redis
def test_the_backend_uses_the_budgeted_connection_class(redis_docker):
    """The pool a RedisBackend builds carries the budgeted class.

    Mutation: build the budgeted class but never install it on the pool,
    which passes every unit test above while production keeps the
    per-address multiplier.
    Oracle: the mixin's presence in the sync pool's connection class MRO.
    The async pool is deliberately left alone: redis-py's async `_connect`
    already wraps its whole address loop in one timeout.
    """
    from _fixtures.redis import redis_test_config
    from cachu.backends.redis import RedisBackend, _ConnectBudgetMixin

    backend = RedisBackend(
        f'redis://{redis_test_config.host}:{redis_test_config.port}/0')
    try:
        assert backend.client.ping() is True
        pool = backend.client.connection_pool
        assert issubclass(pool.connection_class, _ConnectBudgetMixin)
    finally:
        backend.close()
