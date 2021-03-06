import pytest
import redis


class TestPipeline(object):

    def test_pipeline(self, r):
        with r.pipeline() as pipe:
            pipe.set('a', 'a1').get('a').zadd('z', {'z1': 1}).zadd('z', {2: 4})
            pipe.zincrby('z', 1, 'z1').zrange('z', 0, 5, withscores=True)
            assert pipe.execute() == \
                [
                    True,
                    'a1',
                    True,
                    True,
                    2.0,
                    [('z1', 2.0), (2, 4)],
                ]

    def test_pipeline_length(self, r):
        with r.pipeline() as pipe:
            # Initially empty.
            assert len(pipe) == 0
            assert not pipe

            # Fill 'er up!
            pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
            assert len(pipe) == 3
            assert pipe

            # Execute calls reset(), so empty once again.
            pipe.execute()
            assert len(pipe) == 0
            assert not pipe

    def test_pipeline_no_transaction(self, r):
        with r.pipeline(transaction=False) as pipe:
            pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
            assert pipe.execute() == [True, True, True]
            assert r['a'] == 'a1'
            assert r['b'] == 'b1'
            assert r['c'] == 'c1'

    def test_pipeline_no_transaction_watch(self, r):
        r['a'] = 0

        with r.pipeline(transaction=False) as pipe:
            pipe.watch('a')
            a = pipe.get('a')

            pipe.multi()
            pipe.set('a', int(a) + 1)
            assert pipe.execute() == [True]

    def test_pipeline_no_transaction_watch_failure(self, r):
        r['a'] = 0

        with r.pipeline(transaction=False) as pipe:
            pipe.watch('a')
            a = pipe.get('a')

            r['a'] = 'bad'

            pipe.multi()
            pipe.set('a', int(a) + 1)

            with pytest.raises(redis.WatchError):
                pipe.execute()

            assert r['a'] == 'bad'

    def test_exec_error_in_response(self, r):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        r['c'] = 'a'
        with r.pipeline() as pipe:
            pipe.set('a', 1).set('b', '2').lpush('c', 3).set('d', 4)
            result = pipe.execute(raise_on_error=False)

            assert result[0]
            assert r['a'] == 1
            assert result[1]
            assert r['b'] == '2'

            # we can't lpush to a key that's a string value, so this should
            # be a ResponseError exception
            assert isinstance(result[2], redis.ResponseError)
            assert r['c'] == 'a'

            # since this isn't a transaction, the other commands after the
            # error are still executed
            assert result[3]
            assert r['d'] == 4

            # make sure the pipe was restored to a working state
            assert pipe.set('z', 'zzz').execute() == [True]
            assert r['z'] == 'zzz'

    def test_exec_error_raised(self, r):
        r['c'] = 'a'
        with r.pipeline() as pipe:
            pipe.set('a', 1).set('b', 2).lpush('c', 3).set('d', 4)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()
            assert str(ex.value).startswith('Command # 3 (LPUSH c ' + str(r.serialize(3)) +
                                                ') of pipeline caused error: ')

            # make sure the pipe was restored to a working state
            assert pipe.set('z', 'zzz').execute() == [True]
            assert r['z'] == 'zzz'

    def test_parse_error_raised(self, r):
        with r.pipeline() as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set('a', 1).zrem('b').set('b', 2)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert str(ex.value).startswith('Command # 2 (ZREM b) of '
                                                'pipeline caused error: ')

            # make sure the pipe was restored to a working state
            assert pipe.set('z', 'zzz').execute() == [True]
            assert r['z'] == 'zzz'

    def test_watch_succeed(self, r):
        r['a'] = 1
        r['b'] = '2'

        with r.pipeline() as pipe:
            pipe.watch('a', 'b')
            assert pipe.watching
            a_value = pipe.get('a')
            b_value = pipe.get('b')
            assert a_value == 1
            assert b_value == '2'
            pipe.multi()

            pipe.set('c', 3)
            assert pipe.execute() == [True]
            assert not pipe.watching

    def test_watch_failure(self, r):
        r['a'] = 1
        r['b'] = 2

        with r.pipeline() as pipe:
            pipe.watch('a', 'b')
            r['b'] = 3
            pipe.multi()
            pipe.get('a')
            with pytest.raises(redis.WatchError):
                pipe.execute()

            assert not pipe.watching

    def test_unwatch(self, r):
        r['a'] = 1
        r['b'] = 2

        with r.pipeline() as pipe:
            pipe.watch('a', 'b')
            r['b'] = 3
            pipe.unwatch()
            assert not pipe.watching
            pipe.get('a')
            assert pipe.execute() == [1]

    def test_transaction_callable(self, r):
        r['a'] = 1
        r['b'] = 2
        has_run = []

        def my_transaction(pipe):
            a_value = pipe.get('a')
            assert a_value in (1, 2)
            b_value = pipe.get('b')
            assert b_value == 2

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"
            if not has_run:
                r.set('a', 2)
                has_run.append('it has')

            pipe.multi()
            pipe.set('c', a_value + b_value)

        result = r.transaction(my_transaction, 'a', 'b')
        assert result == [True]
        assert r['c'] == 4

    def test_exec_error_in_no_transaction_pipeline(self, r):
        r['a'] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.llen('a')
            pipe.expire('a', 100)

            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert str(ex.value).startswith('Command # 1 (LLEN a) of '
                                                'pipeline caused error: ')

        assert r['a'] == 1

    def test_exec_error_in_no_transaction_pipeline_unicode_command(self, r):
        key = chr(3456) + 'abcd' + chr(3421)
        r[key] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.llen(key)
            pipe.expire(key, 100)

            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            expected = 'Command # 1 (LLEN %s) of pipeline caused error: ' % key
            assert str(ex.value).startswith(expected)

        assert r[key] == 1
