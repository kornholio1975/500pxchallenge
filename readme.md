# CRDT LWW using python
Conflict free replicated data type using last winner wins algorythm using python 2.7
As described [here](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type#LWW-Element-Set_(Last-Write-Wins-Element-Set))

**crdt_main.py** uses python native types only
### Example usage
```python
>>> import crdt_main
>>> data_set = crdt_main.LWWElementSet()
>>> data_set.add('foo', 1532565895.0)
>>> data_set.exists('foo')
>>> data_set.get()
>>> data_set.remove('foo', 1532565941.0)
```
**crdt_redis.py** subclasses from _crdt_main.py_ classes but uses redis for storage instead
### Example usage
```python
>>> import crdt_main
>>> import redis_client.RedisClient
>>> redis_client = redis_client.RedisClient(
        {"host": "redis-19111.c8.us-east-1-3.ec2.cloud.redislabs.com",
        "port": 19111,
        "password": "AL0ndOaB71w4Wo4Ol59lhIHuQou7miqd"})
>>> data_set = crdt_main.LWWElementSetRedis(redis_client)
>>> data_set.add('foo', 1532565895.0)
>>> data_set.exists('foo')
>>> data_set.get()
>>> data_set.remove('foo', 1532565941.0)
```

## Testing
```
$ python test_crdt.py
```
\* _redis tests use redislabs instance defined in redis.json_
