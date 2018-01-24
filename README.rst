serialized-redis
========

Redis python interface that serializes all values using json, pickle, msgpack or a custom serializer.

Getting Started
---------------

.. code-block:: pycon

    >>> import serialized_redis
    >>> r = serialized_redis.JSONSerializedRedis(host='localhost', port=6379, db=0)
    >>> r.set('foo', { 'test': 'dict' })
    True
    >>> r.get('foo')
    {'test': 'dict'}
    
    >>> r = serialized_redis.PickleSerializedRedis(host='localhost', port=6379, db=0)
    >>> r.set('foo', { 'test': 'dict' })
    True
    >>> r.get('foo')
    {'test': 'dict'}
    
    >>> r = serialized_redis.MsgpackSerializedRedis(host='localhost', port=6379, db=0)
    >>> r.set('foo', { 'test': 'dict' })
    True
    >>> r.get('foo')
    {'test': 'dict'}

serialized-redis extends `redis-py <https://github.com/andymccurdy/redis-py>`_ and uses the same interface.

Limitations
-----------

As values are serialized, Redis operations that manipulate or extract data from values are not supported.

* SORT commands may not return correct order depending on the serializer used.
* ZSCAN and SSCAN MATCH option will only work for exact match.
* all lexicographical commands like ZLEXCOUNT, ZREMRANGEBYLEX and ZREVRANGEBYLEX are not supported
* INCR is only supported with JSON serializer


