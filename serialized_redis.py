from json import JSONEncoder, JSONDecoder

from redis.client import string_keys_to_dict, dict_merge, StrictRedis
import redis


class SerializedRedis(redis.StrictRedis):
    '''
        Wrapper to Redis that De/Serializes all values.
    '''

    def __init__(self, *args, encoder=None, decoder=None, decode_responses=True, **kwargs):
        super().__init__(*args, decode_responses=decode_responses, **kwargs)

        if encoder is None:
            self.encode = JSONEncoder().encode
        if decoder is None:
            self.decode = JSONDecoder().decode

        # Chain response callbacks to deserialize output
        FROM_SERIALIZED_CALLBACKS = dict_merge(
                string_keys_to_dict('GET GETSET HGET LPOP RPOPLPUSH BRPOPLPUSH LINDEX SPOP', self.parse_single_object),
                string_keys_to_dict('MGET HVALS HMGET LRANGE SRANDMEMBER', self.parse_list),
                string_keys_to_dict('SMEMBERS SDIFF SINTER SUNION', self.parse_set),
                string_keys_to_dict('HGETALL', self.parse_hgetall),
                string_keys_to_dict('HSCAN', self.parse_hscan),
                string_keys_to_dict('ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE', self.parse_zrange),
                string_keys_to_dict('ZSCAN', self.parse_zscan),
                string_keys_to_dict('BLPOP BRPOP', self.parse_bpop),
        )

        for cmd in FROM_SERIALIZED_CALLBACKS:
            if cmd in self.response_callbacks:
                self.response_callbacks[cmd] = chain_functions(self.response_callbacks[cmd], FROM_SERIALIZED_CALLBACKS[cmd])
            else:
                self.response_callbacks[cmd] = FROM_SERIALIZED_CALLBACKS[cmd]

    def _decode(self, value):
        if value is None or value is '':
            return value
        return self.decode(value)

    def parse_single_object(self, response, **options):
        return self._decode(response)

    def set(self, name, value, *args, **kwargs):
        return super().set(name, self.encode(value), *args, **kwargs)

    def getrange(self, *args, **kwargs):
        raise NotImplementedError('GETRANGE on serialized objects makes no sense.')

    def setrange(self, *args, **kwargs):
        raise NotImplementedError('SETRANGE on serialized objects makes no sense.')

    def setnx(self, name, value, *args, **kwargs):
        return super().setnx(name, self.encode(value), *args, **kwargs)

    def setex(self, name, time, value, *args, **kwargs):
        return super().setex(name, time, self.encode(value), *args, **kwargs)

    def getset(self, name, value, *args, **kwargs):
        return super().getset(name, self.encode(value), *args, **kwargs)

    def mset(self, *args, **kwargs):
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise redis.RedisError('MSET requires **kwargs or a single dict arg')
            kwargs.update(args[0])

        return super().mset(**{k: self.encode(v) for k, v in kwargs.items()})

    def msetnx(self, *args, **kwargs):
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise redis.RedisError('MSETNX requires **kwargs or a single dict arg')
            kwargs.update(args[0])

        return super().msetnx(**{k: self.encode(v) for k, v in kwargs.items()})

    def psetex(self, name, time_ms, value):
        return super().psetex(name, time_ms, self.encode(value))

    def linsert(self, name, where, refvalue, value):
        return super().linsert(name, where, self.encode(refvalue), self.encode(value))

    def smart_get(self, name):
        '''
        Smart get: like get but smarter, returns good type:
            if redis hash, returns python dict
            if redis array, returns python array
            if redis set, return python set
            if redis string, returns python string
        '''
        if not self.exists(name):
            return None
        return  {
                    'set': self.smembers,
                    'hash': self.hgetall,
                    'string': self.get,
                    'list': self.lmembers,
                }[self.type(name)](name)

    def smart_set(self, name, value):
        '''
        Smart set: like set but smarter, sets good type:
            if python dict, uses redis hash
            if python array, uses redis array
            if python set, uses redis set
            otherwise uses redis string
        '''
        with self.pipeline() as pipe:
            pipe.delete(name)

            value_type = type(value).__name__
            if value_type == 'set':
                pipe.delete(name)
                pipe.sadd(name, *value)
            elif value_type == 'list':
                pipe.delete(name)
                pipe.rpush(name, *value)
            elif value_type == 'dict':
                pipe.hmset(name, {k: self.encode(v) for k, v in value.items()})
            else:
                pipe.set(name, self.encode(value))

            pipe.execute()

    # Hashes: fields can be objects
    def parse_hgetall(self, response, **options):
        return { k: self._decode(v) for k, v in response.items() }

    def parse_hscan(self, response, **options):
        cursor, dic = response
        return cursor, { k: self._decode(v) for k, v in dic.items() }

    def hset(self, name, field, value):
        return super().hset(name, field, self.encode(value))

    def hsetnx(self, name, field, value):
        return super().hsetnx(name, field, self.encode(value))

    def hmset(self, name, mapping):
        return super().hmset(name, { field: self.encode(value) for field, value in mapping.items() })

    # Sets
    def sismember(self, name, value):
        return super().sismember(name, self.encode(value))

    def sadd(self, name, *args):
        return super().sadd(name, *list(self.encode(v) for v in args))

    def srem(self, name, *args):
        return super().srem(name, *list(self.encode(v) for v in args))

    def smembers(self, *args, **kwargs):
        return set(super().smembers(*args, **kwargs))

    def smembers_as_list(self, *args, **kwargs):
        return super().smembers(*args, **kwargs)

    def sdiff(self, *args, **kwargs):
        return set(super().sdiff(*args, **kwargs))

    def sdiff_as_list(self, *args, **kwargs):
        return super().sdiff(*args, **kwargs)

    def sinter(self, *args, **kwargs):
        return set(super().sinter(*args, **kwargs))

    def sinter_as_list(self, *args, **kwargs):
        return super().sinter(*args, **kwargs)

    def sunion(self, *args, **kwargs):
        return set(super().sunion(*args, **kwargs))

    def sunion_as_list(self, *args, **kwargs):
        return super().sunion(*args, **kwargs)

    def parse_set(self, response, **options):
        '''
        returns list as members may not be hashable, smember, sdiff fct will turn in into set.
        caller should call smembers/sdiff_as_list if it is known that members may be unhashable and deal with a list instead of a set
        '''
        return [self._decode(v) for v in response]

    # ordered sets
    def zadd(self, name, *args, **kwargs):
        serialized_args = []

        a = iter(args)
        for score, value in zip(a, a):
            serialized_args.append(score)
            serialized_args.append(self.encode(value))
        return super().zadd(name, *serialized_args, **{self.encode(k): v for k, v in kwargs.items()})

    def zrank(self, name, value):
        return super().zrank(name, self.encode(value))

    def zrevrank(self, name, value):
        return super().zrevrank(name, self.encode(value))

    def zrem(self, name, *args):
        return super().zrem(name, *list(self.encode(v) for v in args))

    def zmembers(self, name):
        return self.zrange(name, 0, -1)

    def zscore(self, name, value):
        return super().zscore(name, self.encode(value))

    def zscan(self, name, cursor=0, match=None, count=None, score_cast_func=float):
        # Only support exact match.
        if match is not None and '*' not in match:
            match = self.encode(match)
        return super().zscan(name, cursor=cursor, match=match, count=count, score_cast_func=score_cast_func)

    def zincrby(self, name, value, amount=1):
        return super().zincrby(name, self.encode(value), amount=amount)

    def parse_zrange(self, response, **options):
        if options.get('withscores', False):
            return [(self._decode(v[0]), v[1]) for v in response]
        else:
            return [self._decode(v) for v in response]

    def parse_zscan(self, response, **options):
        cursor, data = response
        return cursor, set((self.decode(value), score) for value, score in data)

    def sort(self, name, start=None, num=None, by=None, get=None,
        desc=False, alpha=False, store=None, groups=False):
        response = super().sort(name, start=start, num=num, by=by, get=get, desc=desc, alpha=alpha, store=store, groups=groups)
        if store is None:
            return self.parse_list(response)
        return response

    # Lists
    def lmembers(self, name):
        return self.lrange(name, 0, -1)

    def parse_list(self, response, **options):
        if isinstance(response, (list, tuple)):
            return [self._decode(v) for v in response]
        return self._decode(response)

    def lpush(self, name, *args):
        return super().lpush(name, *list(self.encode(v) for v in args))

    def lpushx(self, name, value):
        return super().lpushx(name, self.encode(value))

    def lset(self, name, index, value):
        return super().lset(name, index, self.encode(value))

    def lrem(self, name, count, value):
        return super().lrem(name, count, self.encode(value))

    def parse_bpop(self, response, **options):
        if response == None:
            return None
        return (response[0], self._decode(response[1]))

    def rpush(self, name, *args):
        return super().rpush(name, *list(self.encode(v) for v in args))

    def rpop(self, name):
        data = super().rpop(name)
        if data == None:
            return None
        return self._decode(data)

    def publish(self, channel, msg):
        return super().publish(channel, self.encode(msg))

    def type(self, name):
        return super().type(name)

    def pubsub(self):
        return super().pubsub()

    def pipeline(self, transaction=True, shard_hint=None):
        return SerializedRedisPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint
        )


class SerializedRedisPipeline(redis.client.BasePipeline, SerializedRedis):
    "Pipeline for the SerializedRedis class"
    pass


def chain_functions(innerFn, *outerFns):

    def newFn(response, **options):
        new_response = innerFn(response, **options)
        for outerFn in outerFns:
            new_response = outerFn(new_response, **options)
        return new_response

    return newFn
