import collections

from crdt_main import LWWEInset, LWWElementSet
from redis_client import RedisClient

class LWWEInsetRedis(object):
    """Store LWW Elements in CRDT LWWElementSetRedis usilng redis.
    Mimic dict like behavior required for LWWElementSetRedis use cases

    Stores a key value pair where::
        key: any serializble type storing a payload
        value: epoch time expressed as float representing a timestamp
    """

    def __init__(self, redis_client, name):
        """Object requires redis client and a name used as a set name in redis
        arguments::
            redis_client: redis_client.RedisClient  previously initiated Redis client
            name:   str name of ZSET object in redis
        """
        self.__name = name
        self._redisclient = redis_client

    def __setitem__(self, payload, timestamp):
        # test for correct types
        # allow dict like syntax: inset_obj[payload] = timestamp 
        if not isinstance(payload, collections.Hashable):
            raise TypeError('unhashable type {}'.format(type(payload)))
        if not isinstance(timestamp, float):
            raise TypeError('timestamp expected to be an epoch time expressed as float')
        self._redisclient.add_to_set(self.__name, payload, timestamp)

    def __getitem__(self, payload):
        # allow dict like syntax: timestamp = inset_obj[payload]
        return self._redisclient.get_from_set(self.__name, payload)

    def get(self, payload, default=None):
        # allow dict like syntax: timestamp = inset_obj.get(payload, default_value)
        result = self.__getitem__(payload)
        if result is None:
            return default
        return result

    def iterkeys(self):
        # allow dict like syntax: for payload in inset_obj.iterkeys(): pass
        for key, _ in self._redisclient.iterate_over_set(self.__name):
            yield key


class LWWElementSetRedis(LWWElementSet):
    """Conflict Free Replicated Data Type (CRDT) implemented using Last Writer Wins (LWW) algorythm.
    Uses Redis for storage

    Usage::
        >>> import crdt_main
        >>> import redis_client.RedisClient
        >>> redis_client = RedisClient(
                {"host": "redis-19111.c8.us-east-1-3.ec2.cloud.redislabs.com",
                "port": 19111,
                "password": "AL0ndOaB71w4Wo4Ol59lhIHuQou7miqd"})
        >>> data_set = crdt_main.LWWElementSetRedis(redis_client)
        >>> data_set.add('foo', 1532565895)
        >>> data_set.exists('foo')
        >>> data_set.get()
        >>> data_set.remove('foo', 1532565941)
    """

    def __init__(self, redis_params):
        self._redisclient = RedisClient(redis_params)
        self._set_add = LWWEInsetRedis(self._redisclient, 'set_add')
        self._set_remove = LWWEInsetRedis(self._redisclient, 'set_remove')
