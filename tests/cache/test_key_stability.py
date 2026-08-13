"""Tests for cache-key stability and delimiter safety.
"""
import os
import subprocess
import sys

import cachu


def test_set_argument_cache_key_stable_across_process_restarts():
    """A set-valued argument must produce the same cache key across restarts.

    repr() of a set is not order-stable across processes (PYTHONHASHSEED
    randomizes set iteration order), so a naive repr-based key changes on every
    restart and silently never hits the prior entry.
    """
    code = (
        'from cachu.util import make_key_generator\n'
        'def f(tags):\n'
        '    return None\n'
        'gen = make_key_generator(f)\n'
        "key, _ = gen(tags={'us', 'eu', 'asia', 'latam', 'apac', 'emea'})\n"
        'print(key)\n'
    )

    def run(seed: str) -> str:
        env = dict(os.environ)
        env['PYTHONHASHSEED'] = seed
        result = subprocess.run(
            [sys.executable, '-c', code],
            capture_output=True, text=True, env=env)
        assert result.returncode == 0, result.stderr
        return result.stdout

    out_a = run('0')
    out_b = run('1')
    out_c = run('12345')

    assert out_a == out_b == out_c


def test_set_argument_order_does_not_change_key():
    """Two sets with the same members in different insertion order share a key.
    """
    from cachu.util import make_key_generator

    def func(tags):
        return None

    gen = make_key_generator(func)
    key1, _ = gen(tags={'a', 'b', 'c'})
    key2, _ = gen(tags={'c', 'a', 'b'})

    assert key1 == key2


def test_clear_with_glob_metachar_value_does_not_overdelete():
    """A value containing a glob metacharacter must not cause .clear() to evict
    unrelated entries.
    """
    calls = []

    @cachu.cache(ttl=300, backend='memory')
    def func(name: str) -> str:
        calls.append(name)
        return name.upper()

    func('a*b')
    func('axxb')
    assert len(calls) == 2

    func.clear(name='a*b')

    func('axxb')
    assert len(calls) == 2, 'clearing "a*b" must not evict "axxb"'

    func('a*b')
    assert len(calls) == 3


def test_value_containing_escape_sequence_does_not_collide():
    """A value literally containing an escape output (e.g. '%20', '%2A') must
    not collide with the value containing the corresponding raw character.
    """
    from cachu.util import make_key_generator

    def func(q):
        return None

    gen = make_key_generator(func)
    for raw, encoded in [('a b', 'a%20b'), ('a*b', 'a%2Ab'), ('a=b', 'a%3Db'),
                         ('a|b', 'a%7Cb'), ('[x]', '%5Bx%5D')]:
        assert gen(q=raw)[0] != gen(q=encoded)[0], f'{raw!r} collided with {encoded!r}'


def test_distinct_string_values_cached_separately():
    """Two distinct string args (raw vs percent-encoded) get separate entries.
    """
    calls = []

    @cachu.cache(ttl=300, backend='memory')
    def fetch(url: str) -> str:
        calls.append(url)
        return url.upper()

    fetch('q=a b')
    fetch('q=a%20b')
    assert len(calls) == 2, 'percent-encoded value must not hit the raw entry'
