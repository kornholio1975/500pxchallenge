# -*- coding: utf-8 -*-
""" Wrap redis opreations required for CRDT LWW using Redis ZSET type
    Uses key as a store for payload and score as a store for timestamp

    Requires::
        redis library https://github.com/andymccurdy/redis-py
"""

import redis

class RedisClient(object):
    """Wrap redis opreations required for LWWEInsetRedis
    Usage::
        >>> import redis_client.RedisClient
        >>> redis_client = RedisClient(
                {"host": "redis-19111.c8.us-east-1-3.ec2.cloud.redislabs.com",
                "port": 19111,
                "password": "AL0ndOaB71w4Wo4Ol59lhIHuQou7miqd"})
            redis_client.add_to_set('set_add', 'foo', 1532565895.0)
            timestamp = redis_client.get_from_set('set_add', 'foo')
            for payload, timestamp in redis_client.iterate_over_set('set_remove'): pass
            redis_client.flushdb()
    Requires::
        redis library https://github.com/andymccurdy/redis-py
    """

    def __init__(self, params):
        self.__client = redis.StrictRedis(**params)

    def add_to_set(self, set_name, payload, timestamp):
        self.__client.zadd(set_name, timestamp, payload)

    def get_from_set(self, set_name, payload):
        return self.__client.zscore(set_name, payload)

    def iterate_over_set(self, set_name):
        for key, value in self.__client.zscan_iter(set_name):
            yield key, value
    
    def flushdb(self):
        self.__client.flushdb()