import datetime

import pytest
import redis

from .conftest import skip_if_server_version_lt, skip_if_server_version_gte


class TestRedisCommands(object):

    def test_command_on_invalid_key_type(self, r):
        r.lpush('a', '1')
        with pytest.raises(redis.ResponseError):
            r['a']

    def test_delitem(self, r):
        r['a'] = 'foo'
        del r['a']
        assert r.get('a') is None

    def test_smart_get_and_set(self, r):
        # get and set can't be tested independently of each other
        assert r.smart_get('a') is None

        r.smart_set('a', 'str')
        assert r.smart_get('a') == 'str'
        assert r.type('a') == 'string'

        r.smart_set('a', 10)
        assert r.smart_get('a') == 10
        assert r.type('a') == 'string'

        l = [1, '2', '3']
        r.smart_set('a', l)
        assert r.smart_get('a') == l
        assert r.type('a') == 'list'

        s = set([1, '2', '3'])
        r.smart_set('a', s)
        assert r.smart_get('a') == set(s)
        assert r.type('a') == 'set'

        d = {'a': 1, 'b': '2'}
        r.smart_set('a', d)
        assert r.smart_get('a') == d
        assert r.type('a') == 'hash'

    def test_get_and_set(self, r):
        # get and set can't be tested independently of each other
        assert r.get('a') is None
        # byte_string = b('value')
        integer = 5
        unicode_string = chr(3456) + 'abcd' + chr(3421)
        obj = {'list': [1, 2, 3, 'four'], 'int_value': 1, 'dict': dict(a='a', b=2), 'strvalue': 'str'}
        # assert r.set('byte_string', byte_string)
        assert r.set('integer', 5)
        assert r.set('unicode_string', unicode_string)
        assert r.set('obj', obj)
        # assert r.get('byte_string') == byte_string
        assert r.get('integer') == integer
        assert r.get('unicode_string') == unicode_string
        assert r.get('obj') == obj

    def test_getitem_and_setitem(self, r):
        r['a'] = 'bar'
        assert r['a'] == 'bar'

    def test_getitem_raises_keyerror_for_missing_key(self, r):
        with pytest.raises(KeyError):
            r['a']

    def test_getitem_does_not_raise_keyerror_for_empty_string(self, r):
        r['a'] = ""
        assert r['a'] == ""

    def test_get_set_bit(self, r):
        # no value
        assert not r.getbit('a', 5)
        # set bit 5
        assert not r.setbit('a', 5, True)
        assert r.getbit('a', 5)
        # unset bit 4
        assert not r.setbit('a', 4, False)
        assert not r.getbit('a', 4)
        # set bit 4
        assert not r.setbit('a', 4, True)
        assert r.getbit('a', 4)
        # set bit 5 again
        assert r.setbit('a', 5, True)
        assert r.getbit('a', 5)

    def test_getset(self, r):
        assert r.getset('a', 'foo') is None
        assert r.getset('a', 'bar') == 'foo'
        assert r.get('a') == 'bar'

    def test_incr(self, r):
        assert r.incr('a') == 1
        assert r['a'] == 1
        assert r.incr('a') == 2
        assert r['a'] == 2
        assert r.incr('a', amount=5) == 7
        assert r['a'] == 7

    def test_incrby(self, r):
        assert r.incrby('a') == 1
        assert r.incrby('a', 4) == 5
        assert r['a'] == 5

    @skip_if_server_version_lt('2.6.0')
    def test_incrbyfloat(self, r):
        assert r.incrbyfloat('a') == 1.0
        assert r['a'] == 1
        assert r.incrbyfloat('a', 1.1) == 2.1
        assert r['a'] == float(2.1)

    def test_keys(self, r):
        assert r.keys() == []
        keys_with_underscores = set(['test_a', 'test_b'])
        keys = keys_with_underscores.union(set(['testc']))
        for key in keys:
            r[key] = 1
        assert set(r.keys(pattern='test_*')) == keys_with_underscores
        assert set(r.keys(pattern='test*')) == keys

    def test_mget(self, r):
        assert r.mget(['a', 'b']) == [None, None]
        r['a'] = '1'
        r['b'] = '2'
        r['c'] = '3'
        assert r.mget('a', 'other', 'b', 'c') == ['1', None, '2', '3']

    def test_mset(self, r):
        d = {'a': '1', 'b': '2', 'c': 3}
        assert r.mset(d)
        for k, v in d.items():
            assert r[k] == v

    def test_msetnx(self, r):
        d = {'a': 1, 'b': '2', 'c': '3'}
        assert r.msetnx(d)
        d2 = {'a': 'x', 'd': '4'}
        assert not r.msetnx(d2)
        for k, v in d.items():
            assert r[k] == v
        assert r.get('d') is None

    @skip_if_server_version_lt('2.6.0')
    def test_psetex(self, r):
        assert r.psetex('a', 1000, 'value')
        assert r['a'] == 'value'
        assert 0 < r.pttl('a') <= 1000

    @skip_if_server_version_lt('2.6.0')
    def test_psetex_timedelta(self, r):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert r.psetex('a', expire_at, 'value')
        assert r['a'] == 'value'
        assert 0 < r.pttl('a') <= 1000

    def test_rename(self, r):
        r['a'] = '1'
        assert r.rename('a', 'b')
        assert r.get('a') is None
        assert r['b'] == '1'

    def test_renamenx(self, r):
        r['a'] = '1'
        r['b'] = '2'
        assert not r.renamenx('a', 'b')
        assert r['a'] == '1'
        assert r['b'] == '2'

    @skip_if_server_version_lt('2.6.0')
    def test_set_nx(self, r):
        assert r.set('a', '1', nx=True)
        assert not r.set('a', '2', nx=True)
        assert r['a'] == '1'

    @skip_if_server_version_lt('2.6.0')
    def test_set_xx(self, r):
        assert not r.set('a', '1', xx=True)
        assert r.get('a') is None
        r['a'] = 'bar'
        assert r.set('a', '2', xx=True)
        assert r.get('a') == '2'

    @skip_if_server_version_lt('2.6.0')
    def test_set_px(self, r):
        assert r.set('a', '1', px=10000)
        assert r['a'] == '1'
        assert 0 < r.pttl('a') <= 10000
        assert 0 < r.ttl('a') <= 10

    @skip_if_server_version_lt('2.6.0')
    def test_set_px_timedelta(self, r):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert r.set('a', '1', px=expire_at)
        assert 0 < r.pttl('a') <= 1000
        assert 0 < r.ttl('a') <= 1

    @skip_if_server_version_lt('2.6.0')
    def test_set_ex(self, r):
        assert r.set('a', '1', ex=10)
        assert 0 < r.ttl('a') <= 10

    @skip_if_server_version_lt('2.6.0')
    def test_set_ex_timedelta(self, r):
        expire_at = datetime.timedelta(seconds=60)
        assert r.set('a', '1', ex=expire_at)
        assert 0 < r.ttl('a') <= 60

    @skip_if_server_version_lt('2.6.0')
    def test_set_multipleoptions(self, r):
        r['a'] = 'val'
        assert r.set('a', '1', xx=True, px=10000)
        assert 0 < r.ttl('a') <= 10

    def test_setex(self, r):
        assert r.setex('a', 60, '1')
        assert r['a'] == '1'
        assert 0 < r.ttl('a') <= 60

    def test_setnx(self, r):
        assert r.setnx('a', '1')
        assert r['a'] == '1'
        assert not r.setnx('a', '2')
        assert r['a'] == '1'

    def test_getrange(self, r):
        with pytest.raises(NotImplementedError):
            r.getrange('a', 0, 0)

    def test_setrange(self, r):
        with pytest.raises(NotImplementedError):
            r.setrange('a', 5, 'foo')

    def test_type(self, r):
        assert r.type('a') == 'none'
        r['a'] = '1'
        assert r.type('a') == 'string'
        del r['a']
        r.lpush('a', '1')
        assert r.type('a') == 'list'
        del r['a']
        r.sadd('a', '1')
        assert r.type('a') == 'set'
        del r['a']
        r.zadd('a', {'1': 1})
        assert r.type('a') == 'zset'

    # LIST COMMANDS
    def test_blpop(self, r):
        r.rpush('a', '1', '2')
        r.rpush('b', '3', '4')
        t = r.blpop(['b', 'a'], timeout=1)
        assert t == ('b', '3')
        assert r.blpop(['b', 'a'], timeout=1) == ('b', '4')
        assert r.blpop(['b', 'a'], timeout=1) == ('a', '1')
        assert r.blpop(['b', 'a'], timeout=1) == ('a', '2')
        assert r.blpop(['b', 'a'], timeout=1) is None
        r.rpush('c', '1')
        assert r.blpop('c', timeout=1) == ('c', '1')

    def test_brpop(self, r):
        r.rpush('a', '1', '2')
        r.rpush('b', '3', '4')
        assert r.brpop(['b', 'a'], timeout=1) == ('b', '4')
        assert r.brpop(['b', 'a'], timeout=1) == ('b', '3')
        assert r.brpop(['b', 'a'], timeout=1) == ('a', '2')
        assert r.brpop(['b', 'a'], timeout=1) == ('a', '1')
        assert r.brpop(['b', 'a'], timeout=1) is None
        r.rpush('c', '1')
        assert r.brpop('c', timeout=1) == ('c', '1')

    def test_brpoplpush(self, r):
        r.rpush('a', '1', '2')
        r.rpush('b', '3', '4')
        assert r.brpoplpush('a', 'b') == '2'
        assert r.brpoplpush('a', 'b') == '1'
        assert r.brpoplpush('a', 'b', timeout=1) is None
        assert r.lrange('a', 0, -1) == []
        assert r.lrange('b', 0, -1) == ['1', '2', '3', '4']

    def test_brpoplpush_empty_string(self, r):
        r.rpush('a', '')
        assert r.brpoplpush('a', 'b') == ''

    def test_lindex(self, r):
        r.rpush('a', '1', '2', '3')
        assert r.lindex('a', '0') == '1'
        assert r.lindex('a', '1') == '2'
        assert r.lindex('a', '2') == '3'

    def test_linsert(self, r):
        r.rpush('a', '1', '2', '3')
        assert r.linsert('a', 'after', '2', '2.5') == 4
        assert r.lrange('a', 0, -1) == ['1', '2', '2.5', '3']
        assert r.linsert('a', 'before', '2', '1.5') == 5
        assert r.lrange('a', 0, -1) == \
            ['1', '1.5', '2', '2.5', '3']

    def test_llen(self, r):
        r.rpush('a', '1', '2', '3')
        assert r.llen('a') == 3

    def test_lpop(self, r):
        r.rpush('a', '1', 2, '3')
        assert r.lpop('a') == '1'
        assert r.lpop('a') == 2
        assert r.lpop('a') == '3'
        assert r.lpop('a') is None

    def test_lpush(self, r):
        assert r.lpush('a', '1') == 1
        assert r.lpush('a', '2') == 2
        assert r.lpush('a', '3', 4) == 4
        assert r.lrange('a', 0, -1) == [4, '3', '2', '1']

    def test_lpushx(self, r):
        assert r.lpushx('a', '1') == 0
        assert r.lrange('a', 0, -1) == []
        r.rpush('a', '1', 2, '3')
        assert r.lpushx('a', '4') == 4
        assert r.lrange('a', 0, -1) == ['4', '1', 2, '3']

    def test_lrange(self, r):
        r.rpush('a', '1', '2', 3, '4', '5')
        assert r.lrange('a', 0, 2) == ['1', '2', 3]
        assert r.lrange('a', 2, 10) == [3, '4', '5']
        assert r.lrange('a', 0, -1) == ['1', '2', 3, '4', '5']

    def test_lmembers(self, r):
        r.rpush('a', '1', '2', 3, '4', '5')
        assert r.lmembers('a') == ['1', '2', 3, '4', '5']

    def test_lrem(self, r):
        r.rpush('a', 2, '2', '2', '2')
        assert r.lrem('a', 1, 2) == 1
        assert r.lrange('a', 0, -1) == ['2', '2', '2']
        assert r.lrem('a', 0, '2') == 3
        assert r.lrange('a', 0, -1) == []

    def test_lset(self, r):
        r.rpush('a', '1', 2, '3')
        assert r.lrange('a', 0, -1) == ['1', 2, '3']
        assert r.lset('a', 1, ['4'])
        assert r.lrange('a', 0, 2) == ['1', ['4'], '3']

    def test_ltrim(self, r):
        r.rpush('a', 1, '2', '3')
        assert r.ltrim('a', 0, 1)
        assert r.lrange('a', 0, -1) == [1, '2']

    def test_rpop(self, r):
        r.rpush('a', '1', 2, '3')
        assert r.rpop('a') == '3'
        assert r.rpop('a') == 2
        assert r.rpop('a') == '1'
        assert r.rpop('a') is None

    def test_rpoplpush(self, r):
        r.rpush('a', 'a1', 'a2', 3)
        r.rpush('b', 'b1', 'b2', 'b3')
        assert r.rpoplpush('a', 'b') == 3
        assert r.lrange('a', 0, -1) == ['a1', 'a2']
        assert r.lrange('b', 0, -1) == [3 , 'b1', 'b2', 'b3']

    def test_rpush(self, r):
        assert r.rpush('a', '1') == 1
        assert r.rpush('a', 2) == 2
        assert r.rpush('a', '3', 4) == 4
        assert r.lrange('a', 0, -1) == ['1', 2, '3', 4]

    def test_rpushx(self, r):
        assert r.rpushx('a', 'b') == 0
        assert r.lrange('a', 0, -1) == []
        r.rpush('a', '1', '2', '3')
        assert r.rpushx('a', 4) == 4
        assert r.lrange('a', 0, -1) == ['1', '2', '3', 4]

    # SCAN COMMANDS
    @skip_if_server_version_lt('2.8.0')
    def test_scan(self, r):
        r.set('a', 1)
        r.set('b', 2)
        r.set('c', 3)
        cursor, keys = r.scan()
        assert cursor == 0
        assert set(keys) == set(['a', 'b', 'c'])
        _, keys = r.scan(match='a')
        assert set(keys) == set(['a'])

    @skip_if_server_version_lt('2.8.0')
    def test_scan_iter(self, r):
        r.set('a', 1)
        r.set('b', 2)
        r.set('c', 3)
        keys = list(r.scan_iter())
        assert set(keys) == set(['a', 'b', 'c'])
        keys = list(r.scan_iter(match='a'))
        assert set(keys) == set(['a'])

    @skip_if_server_version_lt('2.8.0')
    def test_sscan(self, r):
        r.sadd('a', 1, '2', 3)
        cursor, members = r.sscan('a')
        assert cursor == 0
        assert set(members) == set([1, '2', 3])
        _, members = r.sscan('a', match=1)
        assert set(members) == set([1])

    @skip_if_server_version_lt('2.8.0')
    def test_sscan_iter(self, r):
        r.sadd('a', '1', 2, '3')
        members = list(r.sscan_iter('a'))
        assert set(members) == set(['1', 2, '3'])
        members = list(r.sscan_iter('a', match='1'))
        assert set(members) == set(['1'])

    @skip_if_server_version_lt('2.8.0')
    def test_hscan(self, r):
        r.hmset('a', {'a': 1, 'b': 2, 'c': [3, '4']})
        cursor, dic = r.hscan('a')
        assert cursor == 0
        assert dic == {'a': 1, 'b': 2, 'c': [3, '4']}
        _, dic = r.hscan('a', match='a')
        assert dic == {'a': 1}

    @skip_if_server_version_lt('2.8.0')
    def test_hscan_iter(self, r):
        r.hmset('a', {'a': 1, 'b': '2', 'c': 3})
        dic = dict(r.hscan_iter('a'))
        assert dic == {'a': 1, 'b': '2', 'c': 3}
        dic = dict(r.hscan_iter('a', match='a'))
        assert dic == {'a': 1}

    @skip_if_server_version_lt('2.8.0')
    def test_zscan(self, r):
        r.zadd('a', {'a': 1, 12: 2, 'c': 3.0})
        cursor, pairs = r.zscan('a')
        assert cursor == 0
        assert set(pairs) == set([('a', 1.0), (12, 2.0), ('c', 3.0)])
        _, pairs = r.zscan('a', match='a')
        assert set(pairs) == set([('a', 1.0)])

    @skip_if_server_version_lt('2.8.0')
    def test_zscan_iter(self, r):
        r.zadd('a', {'a': 1, 'b': 2, 'c': 3})
        pairs = list(r.zscan_iter('a'))
        assert set(pairs) == set([('a', 1), ('b', 2), ('c', 3)])
        pairs = list(r.zscan_iter('a', match='a'))
        assert set(pairs) == set([('a', 1)])

    # SET COMMANDS
    def test_sadd(self, r):
        members = set([1, '2', '3'])
        r.sadd('a', *members)
        assert r.smembers('a') == members

    def test_scard(self, r):
        r.sadd('a', '1', '2', '3')
        assert r.scard('a') == 3

    def test_sdiff(self, r):
        r.sadd('a', '1', 2, '3')
        assert r.sdiff('a', 'b') == set(['1', 2, '3'])
        r.sadd('b', 2, '3')
        assert r.sdiff('a', 'b') == set(['1'])

    def test_sdiffstore(self, r):
        r.sadd('a', '1', 2, '3')
        assert r.sdiffstore('c', 'a', 'b') == 3
        assert r.smembers('c') == set(['1', 2, '3'])
        r.sadd('b', 2, '3')
        assert r.sdiffstore('c', 'a', 'b') == 1
        assert r.smembers('c') == set(['1'])

    def test_sinter(self, r):
        r.sadd('a', '1', 2, '3')
        assert r.sinter('a', 'b') == set()
        r.sadd('b', 2, '3')
        assert r.sinter('a', 'b') == set([2, '3'])

    def test_sinterstore(self, r):
        r.sadd('a', '1', 2, '3')
        assert r.sinterstore('c', 'a', 'b') == 0
        assert r.smembers('c') == set()
        r.sadd('b', 2, '3')
        assert r.sinterstore('c', 'a', 'b') == 2
        assert r.smembers('c') == set([2, '3'])

    def test_sismember(self, r):
        r.sadd('a', '1', 2, '3')
        assert r.sismember('a', '1')
        assert r.sismember('a', 2)
        assert r.sismember('a', '3')
        assert not r.sismember('a', '4')

    def test_smembers(self, r):
        r.sadd('a', '1', 2, '3')
        assert r.smembers('a') == set(['1', 2, '3'])

    def test_smove(self, r):
        r.sadd('a', 1, '2')
        r.sadd('b', 'b1', 'b2')
        assert r.smove('a', 'b', 1)
        assert r.smembers('a') == set(['2'])
        assert r.smembers('b') == set(['b1', 'b2', 1])

    def test_spop(self, r):
        s = ['1', 2, '3']
        r.sadd('a', *s)
        value = r.spop('a')
        assert value in s
        assert r.smembers('a') == set(s) - set([value])

    def test_srandmember(self, r):
        s = ['1', 2, '3']
        r.sadd('a', *s)
        assert r.srandmember('a') in s

    @skip_if_server_version_lt('2.6.0')
    def test_srandmember_multi_value(self, r):
        s = ['1', 2, '3']
        r.sadd('a', *s)
        randoms = r.srandmember('a', number=2)
        assert len(randoms) == 2
        assert set(randoms).intersection(s) == set(randoms)

    def test_srem(self, r):
        r.sadd('a', '1', 2, '3', '4')
        assert r.srem('a', '5') == 0
        assert r.srem('a', 2, '4') == 2
        assert r.smembers('a') == set(['1', '3'])

    def test_sunion(self, r):
        r.sadd('a', '1', 2)
        r.sadd('b', 2, '3')
        assert r.sunion('a', 'b') == set(['1', 2, '3'])

    def test_sunionstore(self, r):
        r.sadd('a', '1', 2)
        r.sadd('b', 2, '3')
        assert r.sunionstore('c', 'a', 'b') == 3
        assert r.smembers('c') == set(['1', 2, '3'])

    # SORTED SET COMMANDS
    def test_zadd(self, r):
        r.zadd('a', dict(a1=1, a2=2, a3=3))
        assert r.zrange('a', 0, -1) == ['a1', 'a2', 'a3']

    def test_zcard(self, r):
        r.zadd('a', dict(a1=1, a2=2, a3=3))
        assert r.zcard('a') == 3

    def test_zcount(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3})
        assert r.zcount('a', '-inf', '+inf') == 3
        assert r.zcount('a', 1, 2) == 2
        assert r.zcount('a', '(' + str(1), 2) == 1
        assert r.zcount('a', 1, '(' + str(2)) == 1
        assert r.zcount('a', 10, 20) == 0

    def test_zincrby(self, r):
        r.zadd('a', dict(a1=1, a2=2, a3=3))
        assert r.zincrby('a', 1, 'a2') == 3.0
        assert r.zincrby('a', 5, 'a3') == 8.0
        assert r.zscore('a', 'a2') == 3.0
        assert r.zscore('a', 'a3') == 8.0

    def test_zmembers(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3})
        assert r.zmembers('a') == [1, 'a2', 'a3']

    def test_zinterstore_sum(self, r):
        r.zadd('a', {1: 1, 'a2': 1, 'a3': 1})
        r.zadd('b', {1: 2, 'a2': 2, 'a3': 2})
        r.zadd('c', {1: 6, 'a3': 5, 'a4': 4})
        assert r.zinterstore('d', ['a', 'b', 'c']) == 2
        assert r.zrange('d', 0, -1, withscores=True) == \
            [('a3', 8), (1, 9)]

    def test_zinterstore_max(self, r):
        r.zadd('a', {1: 1, 'a2': 1, 'a3': 1})
        r.zadd('b', {1: 2, 'a2': 2, 'a3': 2})
        r.zadd('c', {1: 6, 'a3': 5, 'a4': 4})
        assert r.zinterstore('d', ['a', 'b', 'c'], aggregate='MAX') == 2
        assert r.zrange('d', 0, -1, withscores=True) == \
            [('a3', 5), (1, 6)]

    def test_zinterstore_min(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3})
        r.zadd('b', {1: 2, 'a2': 3, 'a3': 5})
        r.zadd('c', {1: 6, 'a3': 5, 'a4': 4})
        assert r.zinterstore('d', ['a', 'b', 'c'], aggregate='MIN') == 2
        print(r.zrange('d', 0, -1, withscores=True))
        assert r.zrange('d', 0, -1, withscores=True) == \
            [(1, 1), ('a3', 3.0)]

    def test_zinterstore_with_weight(self, r):
        r.zadd('a', {1: 1, 'a2': 1, 'a3': 1})
        r.zadd('b', {1: 2, 'a2': 2, 'a3': 2})
        r.zadd('c', {1: 6, 'a3': 5, 'a4': 4})
        assert r.zinterstore('d', {'a': 1, 'b': 2, 'c': 3}) == 2
        assert r.zrange('d', 0, -1, withscores=True) == \
            [('a3', 20), (1, 23)]

    def test_zrange(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3})
        assert r.zrange('a', 0, 1) == [1, 'a2']
        assert r.zrange('a', 1, 2) == ['a2', 'a3']

        # withscores
        assert r.zrange('a', 0, 1, withscores=True) == \
            [(1, 1.0), ('a2', 2.0)]
        assert r.zrange('a', 1, 2, withscores=True) == \
            [('a2', 2.0), ('a3', 3.0)]

        # custom score function
        assert r.zrange('a', 0, 1, withscores=True, score_cast_func=int) == \
            [(1, 1), ('a2', 2)]

    def test_zrangebyscore(self, r):
        r.zadd('a', {2: 2, 'a1': 1, 'a3': 3, 'a4': 4, 'a5': 5})
        assert r.zrangebyscore('a', 2, 4) == [2, 'a3', 'a4']

        # slicing with start/num
        assert r.zrangebyscore('a', 2, 4, start=1, num=2) == \
            ['a3', 'a4']

        # withscores
        assert r.zrangebyscore('a', 2, 4, withscores=True) == \
            [(2, 2.0), ('a3', 3.0), ('a4', 4.0)]

        # custom score function
        assert r.zrangebyscore('a', 2, 4, withscores=True,
                               score_cast_func=int) == \
            [(2, 2), ('a3', 3), ('a4', 4)]

    def test_zrank(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3, 'a4': 4, 'a5': 5})
        assert r.zrank('a', 1) == 0
        assert r.zrank('a', 'a2') == 1
        assert r.zrank('a', 'a6') is None

    def test_zrem(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3})
        assert r.zrem('a', 'a2') == 1
        assert r.zrange('a', 0, -1) == [1, 'a3']
        assert r.zrem('a', 'b') == 0
        assert r.zrange('a', 0, -1) == [1, 'a3']

    def test_zrem_multiple_keys(self, r):
        r.zadd('a', {2: 2, 'a1': 1, 'a3': 3, })
        assert r.zrem('a', 'a1', 2) == 2
        assert r.zrange('a', 0, 5) == ['a3']

    def test_zremrangebyrank(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3, 'a4': 4, 'a5': 5})
        assert r.zremrangebyrank('a', 1, 3) == 3
        assert r.zrange('a', 0, 5) == [1, 'a5']

    def test_zremrangebyscore(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3, 'a4': 4, 'a5': 5})
        assert r.zremrangebyscore('a', 2, 4) == 3
        assert r.zrange('a', 0, -1) == [1, 'a5']
        assert r.zremrangebyscore('a', 2, 4) == 0
        assert r.zrange('a', 0, -1) == [1, 'a5']

    def test_zrevrange(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3})
        assert r.zrevrange('a', 0, 1) == ['a3', 'a2']
        assert r.zrevrange('a', 1, 2) == ['a2', 1]

        # withscores
        assert r.zrevrange('a', 0, 1, withscores=True) == \
            [('a3', 3.0), ('a2', 2.0)]
        assert r.zrevrange('a', 1, 2, withscores=True) == \
            [('a2', 2.0), (1, 1.0)]

        # custom score function
        assert r.zrevrange('a', 0, 1, withscores=True,
                           score_cast_func=int) == \
            [('a3', 3.0), ('a2', 2.0)]

    def test_zrevrangebyscore(self, r):
        r.zadd('a', {'a1': 1, 'a2': 2, 3: 3, 'a4': 4, 'a5': 5})
        assert r.zrevrangebyscore('a', 4, 2) == ['a4', 3, 'a2']

        # slicing with start/num
        assert r.zrevrangebyscore('a', 4, 2, start=1, num=2) == \
            [3, 'a2']

        # withscores
        assert r.zrevrangebyscore('a', 4, 2, withscores=True) == \
            [('a4', 4.0), (3, 3.0), ('a2', 2.0)]

        # custom score function
        assert r.zrevrangebyscore('a', 4, 2, withscores=True,
                                  score_cast_func=int) == \
            [('a4', 4), (3, 3), ('a2', 2)]

    def test_zrevrank(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3, 'a4': 4, 'a5': 5})
        assert r.zrevrank('a', 1) == 4
        assert r.zrevrank('a', 'a2') == 3
        assert r.zrevrank('a', 'a6') is None

    def test_zscore(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3})
        assert r.zscore('a', 1) == 1.0
        assert r.zscore('a', 'a2') == 2.0
        assert r.zscore('a', 'a4') is None

    def test_zunionstore_sum(self, r):
        r.zadd('a', {1: 1, 'a2': 1, 'a3': 1})
        r.zadd('b', {1: 2, 'a2': 2, 'a3': 2})
        r.zadd('c', {1: 6, 'a3': 5, 'a4': 4})
        assert r.zunionstore('d', ['a', 'b', 'c']) == 4
        assert r.zrange('d', 0, -1, withscores=True) == \
            [('a2', 3), ('a4', 4), ('a3', 8), (1, 9)]

    def test_zunionstore_max(self, r):
        r.zadd('a', {1: 1, 'a2': 1, 'a3': 1})
        r.zadd('b', {1: 2, 'a2': 2, 'a3': 2})
        r.zadd('c', {1: 6, 'a3': 5, 'a4': 4})
        assert r.zunionstore('d', ['a', 'b', 'c'], aggregate='MAX') == 4
        assert r.zrange('d', 0, -1, withscores=True) == \
            [('a2', 2), ('a4', 4), ('a3', 5), (1, 6)]

    def test_zunionstore_min(self, r):
        r.zadd('a', {1: 1, 'a2': 2, 'a3': 3})
        r.zadd('b', {1: 2, 'a2': 2, 'a3': 4})
        r.zadd('c', {1: 6, 'a3': 5, 'a4': 4})
        assert r.zunionstore('d', ['a', 'b', 'c'], aggregate='MIN') == 4
        assert r.zrange('d', 0, -1, withscores=True) == \
            [(1, 1), ('a2', 2), ('a3', 3), ('a4', 4)]

    def test_zunionstore_with_weight(self, r):
        r.zadd('a', {1: 1, 'a2': 1, 'a3': 1})
        r.zadd('b', {1: 2, 'a2': 2, 'a3': 2})
        r.zadd('c', {1: 6, 'a3': 5, 'a4': 4})
        assert r.zunionstore('d', {'a': 1, 'b': 2, 'c': 3}) == 4
        assert r.zrange('d', 0, -1, withscores=True) == \
            [('a2', 5), ('a4', 12), ('a3', 20), (1, 23)]

    # HYPERLOGLOG TESTS
    @skip_if_server_version_lt('2.8.9')
    def test_pfadd(self, r):
        members = set(['1', 2, '3'])
        assert r.pfadd('a', *members) == 1
        assert r.pfadd('a', *members) == 0
        assert r.pfcount('a') == len(members)

    @skip_if_server_version_lt('2.8.9')
    def test_pfcount(self, r):
        members = set(['1', 2, '3'])
        r.pfadd('a', *members)
        assert r.pfcount('a') == len(members)
        members_b = set([2, '3', '4'])
        r.pfadd('b', *members_b)
        assert r.pfcount('b') == len(members_b)
        assert r.pfcount('a', 'b') == len(members_b.union(members))

    @skip_if_server_version_lt('2.8.9')
    def test_pfmerge(self, r):
        mema = set(['1', 2, '3'])
        memb = set([2, '3', '4'])
        memc = set(['5', '6', '7'])
        r.pfadd('a', *mema)
        r.pfadd('b', *memb)
        r.pfadd('c', *memc)
        r.pfmerge('d', 'c', 'a')
        assert r.pfcount('d') == 6
        r.pfmerge('d', 'b')
        assert r.pfcount('d') == 7

    # HASH COMMANDS
    def test_hget_and_hset(self, r):
        r.hmset('a', {1: 1, '2': 2, '3': 3})
        # field is not serialized: '1' instead of 1
        assert r.hget('a', '1') == 1
        assert r.hget('a', '2') == 2
        assert r.hget('a', '3') == 3

        # field was updated, redis returns 0
        assert r.hset('a', '2', '5') == 0
        assert r.hget('a', '2') == '5'

        # field is new, redis returns 1
        assert r.hset('a', '4', 4) == 1
        assert r.hget('a', '4') == 4

        # key inside of hash that doesn't exist returns null value
        assert r.hget('a', 'b') is None

    def test_hdel(self, r):
        r.hmset('a', {'1': 1, 2: 2, '3': 3})
        assert r.hdel('a', '2') == 1
        assert r.hget('a', '2') is None
        assert r.hdel('a', '1', '3') == 2
        assert r.hlen('a') == 0

    def test_hexists(self, r):
        r.hmset('a', {1: 1, '2': 2, '3': 3})
        assert r.hexists('a', '1')
        assert not r.hexists('a', '4')

    def test_hgetall(self, r):
        h = {'1': '1', 'a2': '2', 'a3': 3}
        r.hmset('a', h)
        assert r.hgetall('a') == h

    def test_hincrby(self, r):
        assert r.hincrby('a', '1') == 1
        assert r.hincrby('a', '1', amount=2) == 3
        assert r.hincrby('a', '1', amount=-2) == 1

    @skip_if_server_version_lt('2.6.0')
    def test_hincrbyfloat(self, r):
        assert r.hincrbyfloat('a', '1') == 1.0
        assert r.hincrbyfloat('a', '1') == 2.0
        assert r.hincrbyfloat('a', '1', 1.2) == 3.2

    def test_hkeys(self, r):
        h = {'a1': '1', 'a2': 2, 'a3': '3'}
        r.hmset('a', h)
        local_keys = list(h.keys())
        remote_keys = r.hkeys('a')
        assert (sorted(local_keys) == sorted(remote_keys))

    def test_hlen(self, r):
        r.hmset('a', {'1': 1, '2': 2, '3': 3})
        assert r.hlen('a') == 3

    def test_hmget(self, r):
        assert r.hmset('a', {'a': 1, 'b': 2, 'c': '3'})
        assert r.hmget('a', 'a', 'b', 'c') == [1, 2, '3']

    def test_hmset(self, r):
        h = {'a': '1', 'b': 2, 'c': '3'}
        assert r.hmset('a', h)
        assert r.hgetall('a') == h

    def test_hsetnx(self, r):
        # Initially set the hash field
        assert r.hsetnx('a', '1', 1)
        assert r.hget('a', '1') == 1
        assert not r.hsetnx('a', '1', 2)
        assert r.hget('a', '1') == 1

    def test_hvals(self, r):
        h = {'a1': '1', 'a2': '2', 'a3': '3'}
        r.hmset('a', h)
        local_vals = list(h.values())
        remote_vals = r.hvals('a')
        assert sorted(local_vals) == sorted(remote_vals)

    @skip_if_server_version_lt('3.2.0')
    def test_hstrlen(self, r):
        r.hmset('a', {'1': 22, '2': '333'})
        # Not Supported
#         assert r.hstrlen('a', '1') == 2
#         assert r.hstrlen('a', '2') == 3

    # SORT
    def test_sort_basic(self, r):
        r.rpush('a', 3, 2, 1, 4)
        assert r.sort('a') == [1, 2, 3, 4]

    def test_sort_limited(self, r):
        r.rpush('a', 3, 2, 1, 4)
        assert r.sort('a', start=1, num=2) == [2, 3]

    def test_sort_by(self, r):
        r['score:1'] = 8
        r['score:2'] = 3
        r['score:3'] = 5
        # Only supported for interger values
        r.rpush('a', 3, 2, 1)
        assert r.sort('a', by='score:*') == [2, 3, 1]

    def test_sort_get(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', 2, 3, 1)
        # Not Supported
#         assert r.sort('a', get='user:*') == ['u1', 'u2', 'u3']

    def test_sort_get_multi(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        # Not Supported
#         assert r.sort('a', get=('user:*', '#')) == \
#             [b('u1'), b('1'), b('u2'), b('2'), b('u3'), b('3')]

    def test_sort_get_groups_two(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        # Not Supported
#         assert r.sort('a', get=('user:*', '#'), groups=True) == \
#             [(b('u1'), b('1')), (b('u2'), b('2')), (b('u3'), b('3'))]

    def test_sort_groups_string_get(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        with pytest.raises(redis.DataError):
            r.sort('a', get='user:*', groups=True)

    def test_sort_groups_just_one_get(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        with pytest.raises(redis.DataError):
            r.sort('a', get=['user:*'], groups=True)

    def test_sort_groups_no_get(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        with pytest.raises(redis.DataError):
            r.sort('a', groups=True)

    def test_sort_groups_three_gets(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r['door:1'] = 'd1'
        r['door:2'] = 'd2'
        r['door:3'] = 'd3'
        r.rpush('a', '2', '3', '1')
        # Not Supported
#         assert r.sort('a', get=('user:*', 'door:*', '#'), groups=True) == \
#             [
#                 ('u1', 'd1', '1'),
#                 ('u2', 'd2', '2'),
#                 ('u3', 'd3', '3')
#         ]

    def test_sort_desc(self, r):
        r.rpush('a', 2, 3, 1)
        assert r.sort('a', desc=True) == [3, 2, 1]

    def test_sort_alpha(self, r):
        r.rpush('a', 'e', 'c', 'b', 'd', 'a')
        assert r.sort('a', alpha=True) == \
            ['a', 'b', 'c', 'd', 'e']

    def test_sort_store(self, r):
        r.rpush('a', 2, 3, 1)
        assert r.sort('a', store='sorted_values') == 3
        assert r.lrange('sorted_values', 0, -1) == [1, 2, 3]

    def test_sort_all_options(self, r):
        r['user:1:username'] = 'zeus'
        r['user:2:username'] = 'titan'
        r['user:3:username'] = 'hermes'
        r['user:4:username'] = 'hercules'
        r['user:5:username'] = 'apollo'
        r['user:6:username'] = 'athena'
        r['user:7:username'] = 'hades'
        r['user:8:username'] = 'dionysus'

        r['user:1:favorite_drink'] = 'yuengling'
        r['user:2:favorite_drink'] = 'rum'
        r['user:3:favorite_drink'] = 'vodka'
        r['user:4:favorite_drink'] = 'milk'
        r['user:5:favorite_drink'] = 'pinot noir'
        r['user:6:favorite_drink'] = 'water'
        r['user:7:favorite_drink'] = 'gin'
        r['user:8:favorite_drink'] = 'apple juice'

        r.rpush('gods', '5', '8', '3', '1', '2', '7', '6', '4')
        num = r.sort('gods', start=2, num=4, by='user:*:username',
                     get='user:*:favorite_drink', desc=True, alpha=True,
                     store='sorted')
        assert num == 4
        # Not Supported
#         assert r.lrange('sorted', 0, 10) == \
#             [b('vodka'), b('milk'), b('gin'), b('apple juice')]

    def test_cluster_addslots(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('ADDSLOTS', 1) is True

    def test_cluster_count_failure_reports(self, mock_cluster_resp_int):
        assert isinstance(mock_cluster_resp_int.cluster(
            'COUNT-FAILURE-REPORTS', 'node'), int)

    def test_cluster_countkeysinslot(self, mock_cluster_resp_int):
        assert isinstance(mock_cluster_resp_int.cluster(
            'COUNTKEYSINSLOT', 2), int)

    def test_cluster_delslots(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('DELSLOTS', 1) is True

    def test_cluster_failover(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('FAILOVER', 1) is True

    def test_cluster_forget(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('FORGET', 1) is True

    def test_cluster_info(self, mock_cluster_resp_info):
        assert isinstance(mock_cluster_resp_info.cluster('info'), dict)

    def test_cluster_keyslot(self, mock_cluster_resp_int):
        assert isinstance(mock_cluster_resp_int.cluster(
            'keyslot', 'asdf'), int)

    def test_cluster_meet(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('meet', 'ip', 'port', 1) is True

    def test_cluster_nodes(self, mock_cluster_resp_nodes):
        assert isinstance(mock_cluster_resp_nodes.cluster('nodes'), dict)

    def test_cluster_replicate(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('replicate', 'nodeid') is True

    def test_cluster_reset(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('reset', 'hard') is True

    def test_cluster_saveconfig(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('saveconfig') is True

    def test_cluster_setslot(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('setslot', 1,
                                            'IMPORTING', 'nodeid') is True

    def test_cluster_slaves(self, mock_cluster_resp_slaves):
        assert isinstance(mock_cluster_resp_slaves.cluster(
            'slaves', 'nodeid'), dict)

    # GEO COMMANDS
    @skip_if_server_version_lt('3.2.0')
    def test_geoadd(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        assert r.geoadd('barcelona', *values) == 2
        assert r.zcard('barcelona') == 2

    @skip_if_server_version_lt('3.2.0')
    def test_geoadd_invalid_params(self, r):
        with pytest.raises(redis.RedisError):
            r.geoadd('barcelona', *(1, 2))

    @skip_if_server_version_lt('3.2.0')
    def test_geodist(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        assert r.geoadd('barcelona', *values) == 2
        assert r.geodist('barcelona', 'place1', 'place2') == 3067.4157

    @skip_if_server_version_lt('3.2.0')
    def test_geodist_units(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)
        assert r.geodist('barcelona', 'place1', 'place2', 'km') == 3.0674

    @skip_if_server_version_lt('3.2.0')
    def test_geodist_invalid_units(self, r):
        with pytest.raises(redis.RedisError):
            assert r.geodist('x', 'y', 'z', 'inches')

    @skip_if_server_version_lt('3.2.0')
    def test_geohash(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)
        assert r.geohash('barcelona', 'place1', 'place2') == \
            ['sp3e9yg3kd0', 'sp3e9cbc3t0']

    @skip_if_server_version_lt('3.2.0')
    def test_geopos(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)
        # redis uses 52 bits precision, hereby small errors may be introduced.
        assert r.geopos('barcelona', 'place1', 'place2') == \
            [(2.19093829393386841, 41.43379028184083523),
             (2.18737632036209106, 41.40634178640635099)]

    @skip_if_server_version_lt('4.0.0')
    def test_geopos_no_value(self, r):
        assert r.geopos('barcelona', 'place1', 'place2') == [None, None]

    @skip_if_server_version_lt('3.2.0')
    @skip_if_server_version_gte('4.0.0')
    def test_old_geopos_no_value(self, r):
        assert r.geopos('barcelona', 'place1', 'place2') == []

    @skip_if_server_version_lt('3.2.0')
    def test_georadius(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)
        assert r.georadius('barcelona', 2.191, 41.433, 1000) == ['place1']

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_no_values(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)
        assert r.georadius('barcelona', 1, 2, 1000) == []

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_units(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)
        assert r.georadius('barcelona', 2.191, 41.433, 1, unit='km') == \
            ['place1']

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_with(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)

        # test a bunch of combinations to test the parse response
        # function.
        assert r.georadius('barcelona', 2.191, 41.433, 1, unit='km',
                           withdist=True, withcoord=True, withhash=True) == \
            [['place1', 0.0881, 3471609698139488,
              (2.19093829393386841, 41.43379028184083523)]]

        assert r.georadius('barcelona', 2.191, 41.433, 1, unit='km',
                           withdist=True, withcoord=True) == \
            [['place1', 0.0881,
              (2.19093829393386841, 41.43379028184083523)]]

        assert r.georadius('barcelona', 2.191, 41.433, 1, unit='km',
                           withhash=True, withcoord=True) == \
            [['place1', 3471609698139488,
              (2.19093829393386841, 41.43379028184083523)]]

        # test no values.
        assert r.georadius('barcelona', 2, 1, 1, unit='km',
                           withdist=True, withcoord=True, withhash=True) == []

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_count(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)
        assert r.georadius('barcelona', 2.191, 41.433, 3000, count=1) == \
            ['place1']

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_sort(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)
        assert r.georadius('barcelona', 2.191, 41.433, 3000, sort='ASC') == \
            ['place1', 'place2']
        assert r.georadius('barcelona', 2.191, 41.433, 3000, sort='DESC') == \
            ['place2', 'place1']

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_store(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)
        r.georadius('barcelona', 2.191, 41.433, 1000, store='places_barcelona')
        assert r.zrange('places_barcelona', 0, -1) == ['place1']

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_store_dist(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)
        r.georadius('barcelona', 2.191, 41.433, 1000,
                    store_dist='places_barcelona')
        # instead of save the geo score, the distance is saved.
        assert r.zscore('places_barcelona', 'place1') == 88.05060698409301

    @skip_if_server_version_lt('3.2.0')
    def test_georadiusmember(self, r):
        values = (2.1909389952632, 41.433791470673, 'place1') + \
                 (2.1873744593677, 41.406342043777, 'place2')

        r.geoadd('barcelona', *values)
        assert r.georadiusbymember('barcelona', 'place1', 4000) == \
            ['place2', 'place1']
        assert r.georadiusbymember('barcelona', 'place1', 10) == ['place1']

        assert r.georadiusbymember('barcelona', 'place1', 4000,
                                   withdist=True, withcoord=True,
                                   withhash=True) == \
            [['place2', 3067.4157, 3471609625421029,
                (2.187376320362091, 41.40634178640635)],
             ['place1', 0.0, 3471609698139488,
                 (2.1909382939338684, 41.433790281840835)]]
