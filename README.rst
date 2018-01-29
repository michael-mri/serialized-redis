serialized-redis
================

Redis python interface that serializes all values using json, pickle, msgpack or a custom serializer.

.. image:: https://secure.travis-ci.org/michael-mri/serialized-redis.svg?branch=master
        :target: http://travis-ci.org/michael-mri/serialized-redis

.. image:: https://codecov.io/gh/michael-mri/serialized-redis/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/michael-mri/serialized-redis

Getting Started
---------------

Installation
~~~~~~~~~~~~

::

    pip install serialized-redis-interface

Usage
~~~~~

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

``serialized-redis`` extends `redis-py <https://github.com/andymccurdy/redis-py>`_ and uses the same interface.

Most commands, Piplines and PubSub are supported and take care of serializing and deserializing values.

``msgpack`` must be installed in order to use ``MsgpackSerializedRedis``.

All strings are python str.

Limitations
-----------

As values are serialized, Redis operations that manipulate or extract data from values are not supported.

* SORT commands may not return correct order depending on the serializer used.
* ZSCAN and SSCAN MATCH option will only work for exact match.
* STRLENGTH and HSTRLENGTH will return the length of the serialized value.
* all lexicographical commands like ZLEXCOUNT, ZREMRANGEBYLEX and ZREVRANGEBYLEX are not supported
* INCR is only supported with JSON serializer
* fields of Redis hashes are not serialized

Extra Methods
-------------


* ``smembers_as_list``, ``sdiff_as_list``, ``sinter_as_list``, ``sunion_as_list`` can be used when members of the redis
  set may not be hashable once deserialized.

  .. code-block:: pycon

    >>> r = serialized_redis.JSONSerializedRedis() 
    >>> r.sadd('myset', {'dict': 1})
    1
    >>> r.smembers('myset')
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "/home/michael/workspace/Origin_Nexus/serialized_redis/serialized_redis/__init__.py", line 176, in smembers
        return set(super().smembers(*args, **kwargs))
    TypeError: unhashable type: 'dict'
    >>> r.smembers_as_list('myset')
    [{'dict': 1}]

* ``smart_get`` and ``smart_set`` can be used to retrieve and store python structure with their redis counterpart:

  * python ``list`` as redis LIST
  * python ``set`` as redis SET
  * python ``dict`` as redis HASH, fields will not be (de)serialized.

Custom Serializer
-----------------

You can use your own seriliazing and deserializing functions:


.. code-block:: pycon

    >>> r = serialized_redis.SerializedRedis(serialization_fn=my_serializer, deserialization_fn=my_deserializer)

If your deserializer function expects python 3 strings instead of bytes, you can add ``decode_responses=True`` parameter.

Decoding bytes to str when required is the responsability of the deserialization function.
