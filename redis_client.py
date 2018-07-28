import redis

class RedisClient(object):

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