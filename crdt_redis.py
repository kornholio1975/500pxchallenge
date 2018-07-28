import collections

from crdt_main import LWWEInset, LWWElementSet
from redis_client import RedisClient

class LWWEInsetRedis(object):

    def __init__(self, redis_client, name):
        self.__name = name
        self._redisclient = redis_client

    def __setitem__(self, payload, timestamp):
        if not isinstance(payload, collections.Hashable):
            raise TypeError('unhashable type {}'.format(type(payload)))
        if not isinstance(timestamp, float):
            raise TypeError('timestamp expected to be an epoch time expressed as float')
        self._redisclient.add_to_set(self.__name, payload, timestamp)

    def __getitem__(self, payload):
        return self._redisclient.get_from_set(self.__name, payload)

    def get(self, payload, default=None):
        result = self.__getitem__(payload)
        if result is None:
            return default
        return result

    def iterkeys(self):
        for key, _ in self._redisclient.iterate_over_set(self.__name):
            yield key


class LWWElementSetRedis(LWWElementSet):

    def __init__(self, redis_params):
        self._redisclient = RedisClient(redis_params)
        self._set_add = LWWEInsetRedis(self._redisclient, 'set_add')
        self._set_remove = LWWEInsetRedis(self._redisclient, 'set_remove')
