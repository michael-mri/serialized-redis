import functools
from json import JSONEncoder, JSONDecoder

import redis
from redis.client import string_keys_to_dict, dict_merge

__version__ = '0.3.1'


class SerializedRedis(redis.Redis):
    '''
        Wrapper to Redis that De/Serializes all values.
    '''

    def __init__(self, *args, serialize_fn, deserialize_fn, **kwargs):
        super().__init__(*args, **kwargs)

        self.serialize_fn = serialize_fn
        self.deserialize_fn = deserialize_fn

        # Chain response callbacks to deserialize output
        FROM_SERIALIZED_CALLBACKS = dict_merge(
                string_keys_to_dict('KEYS TYPE SCAN HKEYS', self.decode),
                string_keys_to_dict('MGET HVALS HMGET LRANGE SRANDMEMBER GET GETSET HGET LPOP '
                                    'RPOPLPUSH BRPOPLPUSH LINDEX SPOP', self.parse_list),
                string_keys_to_dict('SMEMBERS SDIFF SINTER SUNION', self.parse_set),
                string_keys_to_dict('HGETALL', self.parse_hgetall),
                string_keys_to_dict('HSCAN', self.parse_hscan),
                string_keys_to_dict('SSCAN', self.parse_sscan),
                string_keys_to_dict('ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE', self.parse_zrange),
                string_keys_to_dict('ZSCAN', self.parse_zscan),
                string_keys_to_dict('BLPOP BRPOP', self.parse_bpop),
                {
                    'PUBSUB CHANNELS': self.decode,
                    'PUBSUB NUMSUB': self.decode,
                },
        )

        for cmd in FROM_SERIALIZED_CALLBACKS:
            if cmd in self.response_callbacks:
                self.response_callbacks[cmd] = chain_functions(self.response_callbacks[cmd], FROM_SERIALIZED_CALLBACKS[cmd])
            else:
                self.response_callbacks[cmd] = FROM_SERIALIZED_CALLBACKS[cmd]

        # For the following we call first our callback as the redis-py callback returns nativestr, which may no be what we want
        for cmd in 'GEORADIUS', 'GEORADIUSBYMEMBER':
            self.response_callbacks[cmd] = chain_functions(self.parse_georadius, self.response_callbacks[cmd])

    def serialize(self, value):
        return self.serialize_fn(value)

    def deserialize(self, value):
        if value is None or value is '':
            return value
        return self.deserialize_fn(value)

    def decode(self, value):
        "Return a unicode string from the byte representation"
        if isinstance(value, bytes):
            value = value.decode()
        elif isinstance(value, list):
            return [self.decode(v) for v in value]
        elif isinstance(value, tuple):
            return tuple(self.decode(v) for v in value)
        elif isinstance(value, dict):
            return {k: self.decode(v) for k, v in value.items()}
        return value

    def set(self, name, value, *args, **kwargs):
        return super().set(name, self.serialize(value), *args, **kwargs)

    def getrange(self, *args, **kwargs):
        raise NotImplementedError('GETRANGE on serialized objects makes no sense.')

    def setrange(self, *args, **kwargs):
        raise NotImplementedError('SETRANGE on serialized objects makes no sense.')

    def setnx(self, name, value, *args, **kwargs):
        return super().setnx(name, self.serialize(value), *args, **kwargs)

    def setex(self, name, time, value, *args, **kwargs):
        return super().setex(name, time, self.serialize(value), *args, **kwargs)

    def getset(self, name, value, *args, **kwargs):
        return super().getset(name, self.serialize(value), *args, **kwargs)

    def mset(self, mapping):
        return super().mset({k: self.serialize(v) for k, v in mapping.items()})

    def msetnx(self, mapping):
        return super().msetnx({k: self.serialize(v) for k, v in mapping.items()})

    def psetex(self, name, time_ms, value):
        return super().psetex(name, time_ms, self.serialize(value))

    def linsert(self, name, where, refvalue, value):
        return super().linsert(name, where, self.serialize(refvalue), self.serialize(value))

    def smart_get(self, name):
        '''
        Returns python type corresponding to redis type:
            if redis hash, returns python dict with values deserialized
            if redis array, returns python array with members deserialized
            if redis set, returns python set with members deserialized
            if redis sorted set, returns a list
            if redis string, returns a python object from deserialization
        '''
        if not self.exists(name):
            return None
        return  {
                    'set': self.smembers,
                    'hash': self.hgetall,
                    'string': self.get,
                    'list': self.lmembers,
                    'zset': self.zmembers,
                }[self.type(name)](name)

    def smart_set(self, name, value):
        '''
        Saves value using appropriate Redis type:
            if python dict, uses redis hash, serializing values (not keys)
            if python list, uses redis array, serializing members
            if python set, uses redis set, serializing members
            otherwise uses redis string, serializing ``value``
        '''
        with self.pipeline() as pipe:
            pipe.delete(name)

            value_type = type(value)
            if value_type is set:
                pipe.sadd(name, *value)
            elif value_type is list:
                pipe.rpush(name, *value)
            elif value_type is dict:
                pipe.hmset(name, value)
            else:
                pipe.set(name, value)

            pipe.execute()

    # Hashes: fields can be objects
    def parse_hgetall(self, response, **options):
        return { self.decode(k): self.deserialize(v) for k, v in response.items() }

    def parse_georadius(self, response, **options):
        if options['store'] or options['store_dist']:
            # `store` and `store_diff` cant be combined
            # with other command arguments.
            return response

        if type(response) != list:
            response_list = [response]
        else:
            response_list = response

        if not options['withdist'] and not options['withcoord']\
                and not options['withhash']:
            print('respos', response, response_list)
            return [self.deserialize(r) for r in response_list]

        for r in response_list:
            r[0] = self.deserialize(r[0])

        return response_list

    def parse_hscan(self, response, **options):
        cursor, dic = response
        return cursor, { self.decode(k): self.deserialize(v) for k, v in dic.items() }

    def hset(self, name, field, value):
        return super().hset(name, field, self.serialize(value))

    def hsetnx(self, name, field, value):
        return super().hsetnx(name, field, self.serialize(value))

    def hmset(self, name, mapping):
        return super().hmset(name, { field: self.serialize(value) for field, value in mapping.items() })

    # Sets
    def sismember(self, name, value):
        return super().sismember(name, self.serialize(value))

    def sadd(self, name, *args):
        return super().sadd(name, *list(self.serialize(v) for v in args))

    def srem(self, name, *args):
        return super().srem(name, *list(self.serialize(v) for v in args))

    def smembers(self, *args, **kwargs):
        return set(super().smembers(*args, **kwargs))

    def smembers_as_list(self, *args, **kwargs):
        """
        Returns SMEMBERS as python list instead of set.
        To be used when deserialized members may not hashable.
        """
        return super().smembers(*args, **kwargs)

    def sdiff(self, *args, **kwargs):
        return set(super().sdiff(*args, **kwargs))

    def sdiff_as_list(self, *args, **kwargs):
        """
        Returns SDIFF as python list instead of set.
        To be used when deserialized members may not hashable.
        """
        return super().sdiff(*args, **kwargs)

    def sinter(self, *args, **kwargs):
        return set(super().sinter(*args, **kwargs))

    def sinter_as_list(self, *args, **kwargs):
        """
        Returns SINTER as python list instead of set.
        To be used when deserialized members may not hashable.
        """
        return super().sinter(*args, **kwargs)

    def sunion(self, *args, **kwargs):
        return set(super().sunion(*args, **kwargs))

    def sunion_as_list(self, *args, **kwargs):
        """
        Returns SUNION as python list instead of set.
        To be used when deserialized members may not hashable.
        """
        return super().sunion(*args, **kwargs)

    def smove(self, src, dst, value):
        return super().smove(src, dst, self.serialize(value))

    def parse_set(self, response, **options):
        '''
        returns list as members may not be hashable, smember, sdiff fct will turn in into set.
        caller should call smembers/sdiff_as_list if it is known that members may be unhashable and deal with a list instead of a set
        '''
        return [self.deserialize(v) for v in response]

    # ordered sets
    def zadd(self, name, mapping, **kwargs):
        serialized_mapping = {}

        for value, score in mapping.items():
            serialized_mapping[self.serialize(value)] = score
        return super().zadd(name, serialized_mapping, **kwargs)

    def zrank(self, name, value):
        return super().zrank(name, self.serialize(value))

    def zrevrank(self, name, value):
        return super().zrevrank(name, self.serialize(value))

    def zrem(self, name, *args):
        return super().zrem(name, *list(self.serialize(v) for v in args))

    def zmembers(self, name, **kwargs):
        '''
        returns all members of Sorted Set.
        convenience function for zrange(name, 0, -1)
        '''
        return self.zrange(name, 0, -1, **kwargs)

    def zscore(self, name, value):
        return super().zscore(name, self.serialize(value))

    def zscan(self, name, cursor=0, match=None, count=None,
              score_cast_func=float):
        # Only support exact match.
        if match is not None:
            match = self.serialize(match)
        return super().zscan(name, cursor=cursor, match=match, count=count,
                             score_cast_func=score_cast_func)

    zlexcount = None
    "Not Supported"

    zrangebylex = None
    "Not Supported"

    zrevrangebylex = None
    "Not Supported"

    zremrangebylex = None
    "Not Supported"

    def sscan(self, name, cursor=0, match=None, count=None):
        if match is not None:
            match = self.serialize(match)
        return super().sscan(name, cursor=cursor, match=match, count=count)

    def zincrby(self, name, amount, value):
        return super().zincrby(name, amount, self.serialize(value))

    def parse_zrange(self, response, **options):
        if options.get('withscores', False):
            return [(self.deserialize(v[0]), v[1]) for v in response]
        else:
            return [self.deserialize(v) for v in response]

    def parse_zscan(self, response, **options):
        cursor, data = response
        return cursor, [(self.deserialize(value), score) for value, score in data]

    def parse_sscan(self, response, **options):
        cursor, data = response
        return cursor, set(self.deserialize(value) for value in data)

    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None, groups=False):
        response = super().sort(name, start=start, num=num, by=by, get=get, desc=desc, alpha=alpha, store=store, groups=groups)
        if store is None:
            return self.parse_list(response)
        return response

    # Lists
    def lmembers(self, name):
        '''
        Returns list of all members of list ``name``.
        Convenience function for lrange(name, 0, -1).
        '''
        return self.lrange(name, 0, -1)

    def parse_list(self, response, **options):
        if isinstance(response, (list, tuple)):
            return [self.deserialize(v) for v in response]
        return self.deserialize(response)

    def lpush(self, name, *args):
        return super().lpush(name, *list(self.serialize(v) for v in args))

    def lpushx(self, name, value):
        return super().lpushx(name, self.serialize(value))

    def lset(self, name, index, value):
        return super().lset(name, index, self.serialize(value))

    def lrem(self, name, count, value):
        return super().lrem(name, count, self.serialize(value))

    def parse_bpop(self, response, **options):
        if response == None:
            return None
        return (self.decode(response[0]), self.deserialize(response[1]))

    def rpush(self, name, *args):
        return super().rpush(name, *list(self.serialize(v) for v in args))

    def rpushx(self, name, value):
        return super().rpushx(name, self.serialize(value))

    def rpop(self, name):
        data = super().rpop(name)
        if data == None:
            return None
        return self.deserialize(data)

    def geoadd(self, name, *values):
        serialized_values = []
        for i, v in enumerate(values):
            if i % 3 == 2:
                v = self.serialize(v)
            serialized_values.append(v)

        return super().geoadd(name, *serialized_values)

    def geopos(self, name, *values):
        return super().geopos(name, *(self.serialize(v) for v in values))

    def georadiusbymember(self, name, member, radius, unit=None,
                          withdist=False, withcoord=False, withhash=False,
                          count=None, sort=None, store=None, store_dist=None):
        return super().georadiusbymember(name, self.serialize(member), radius, unit=unit, withdist=withdist,
                                         withcoord=withcoord, withhash=withhash, count=count, sort=sort,
                                         store=store, store_dist=store_dist)

    def geodist(self, name, place1, place2, unit=None):
        return super().geodist(name, self.serialize(place1), self.serialize(place2), unit=unit)

    def geohash(self, name, *values):
        return super().geohash(name, *(self.serialize(v) for v in values))

    def publish(self, channel, msg):
        return super().publish(channel, self.serialize(msg))

    def pipeline(self, transaction=True, shard_hint=None):
        serialize_fn = self.serialize_fn

        # create a Pipeline class based on our class and provide our serialize function
        # deserialize not required as it is called only form response_callbacks which
        # refer to current SerializedRedis instance
        class SerializedRedisPipeline(redis.client.Pipeline, type(self)):
            "Pipeline for the SerializedRedis class"

            def serialize_fn(self, value):
                return serialize_fn(value)

        return SerializedRedisPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint
        )

    def pubsub(self, **kwargs):
        return PubSub(self.connection_pool, serialized_redis=self, **kwargs)


class PubSub(redis.client.PubSub):

    def __init__(self, *args, serialized_redis, **kwargs):
        super().__init__(*args, **kwargs)
        self.serialized_redis = serialized_redis

    def handle_message(self, response, ignore_subscribe_messages=False):
        response[0] = self.serialized_redis.decode(response[0])
        response[1] = self.serialized_redis.decode(response[1])
        response_type = response[0]
        if response_type == 'pmessage':
            response[2] = self.serialized_redis.decode(response[2])
            response[3] = self.serialized_redis.deserialize(response[3])
        elif response_type == 'message':
            response[2] = self.serialized_redis.deserialize(response[2])
        return super().handle_message(response, ignore_subscribe_messages=ignore_subscribe_messages)

    def _normalize_keys(self, data):
        # We only treat str...
        return data


def chain_functions(innerFn, *outerFns):

    def newFn(response, **options):
        new_response = innerFn(response, **options)
        for outerFn in outerFns:
            new_response = outerFn(new_response, **options)
        return new_response

    return newFn


class JSONSerializedRedis(SerializedRedis):
    '''
    Redis connection that serializes and deserializes all values using json.

    dict keys are normalized using sort_keys=True which means in a same dict, keys must be sortable.
    '''

    def __init__(self, *args, **kwargs):
        import json
        serialize_fct = json.JSONEncoder(sort_keys=True).encode
        super().__init__(*args, serialize_fn=serialize_fct, deserialize_fn=json.loads, decode_responses=True, **kwargs)


class PickleSerializedRedis(SerializedRedis):
    '''
    Redis connection that serializes and deserializes all values using pickle.

    dict keys are normalized using sort_keys=True which means in a same dict, keys must be sortable.
    '''

    def __init__(self, *args, **kwargs):
        import pickle
        super().__init__(*args, serialize_fn=pickle.dumps, deserialize_fn=pickle.loads, **kwargs)

    def incrby(self, name, amount=1):
        raise NotImplementedError('Operation not supported with pickle serializer')

    def incrbyfloat(self, name, amount=1.0):
        raise NotImplementedError('Operation not supported with pickle serializer')

    def sort(self, name, start=None, num=None, by=None, get=None,
        desc=False, alpha=False, store=None, groups=False):
        # once pickled aplha order seems respected for numbers but not for pickled strings
        if alpha:
            raise NotImplementedError('Server side string comparison not supported with pickle serializer')
        if by is not None:
            raise NotImplementedError('Server side string comparison using weights with pickle serializer')
        # for number comparison, force alpha to True
        return super().sort(name, start=start, num=num, by=by, get=get, desc=desc, alpha=True, store=store, groups=groups)


class MsgpackSerializedRedis(SerializedRedis):

    def __init__(self, *args, **kwargs):
        import msgpack
        deserialize_fct = functools.partial(msgpack.unpackb, raw=False)
        super().__init__(*args, serialize_fn=msgpack.dumps, deserialize_fn=deserialize_fct, **kwargs)

    def incrby(self, name, amount=1):
        raise NotImplementedError('Operation not supported with msgpack serializer')

    def incrbyfloat(self, name, amount=1.0):
        raise NotImplementedError('Operation not supported with pickle serializer')

    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None, groups=False):
        # once pickled aplha order seems respected for numbers but not for pickled strings
        if alpha:
            raise NotImplementedError('Server side string comparison not supported with pickle serializer')
        if by is not None:
            raise NotImplementedError('Server side string comparison using weights with pickle serializer')
        # for number comparison, force alpha to True
        return super().sort(name, start=start, num=num, by=by, get=get, desc=desc, alpha=True, store=store, groups=groups)
