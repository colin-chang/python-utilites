from redis import StrictRedis
from collections.abc import Iterable


class RedisHelper(object):
    def __init__(self, **kwargs):
        """
        :param kwargs: Redis配置
        """

        self.__redis = StrictRedis(**kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__redis.close()

    def __del__(self):
        self.__redis.close()

    def string_set(self, name, value, **kwargs):
        return self.__redis.set(name, value, **kwargs)

    def string_get(self, name):
        return self.__decode(self.__redis.get(name))

    def increase(self, name, amount=1):
        """
        自增
        """
        return self.__redis.incr(name, amount)

    def decrease(self, name, amount=1):
        """
        自减
        """
        return self.__redis.decr(name, amount)

    def enqueue(self, name, *values):
        return self.__redis.rpush(name, *values)

    def dequeue(self, name, count=None):
        return self.__decode(self.__redis.lpop(name, count))

    def peek_range(self, name, start=0, end=0):
        """
        从队列中读取数据而不出队
        """
        return self.__decode(self.__redis.lrange(name, start, end))

    def set_add(self, name, *values):
        return self.__redis.sadd(name, *values)

    def set_remove(self, name, *values):
        return self.__redis.srem(name, *values)

    def set_members(self, name):
        return self.__decode(self.__redis.smembers(name))

    def zset_add(self, name, mapping, **kwargs):
        return self.__redis.zadd(name, mapping, **kwargs)

    def zset_remove(self, name, *values):
        return self.__redis.zrem(name, *values)

    def zset_remove_range(self, name, min, max):
        return self.__redis.zremrangebyscore(name, min, max)

    def zset_range(self, name, start, end, **kwargs):
        return self.__decode(self.__redis.zrange(name, start, end, **kwargs))

    def zset_range_byscore(self, name, min, max, **kwargs):
        return self.__decode(self.__redis.zrangebyscore(name, min, max, **kwargs))

    def zset_score(self, name, member):
        return self.__decode(self.__redis.zscore(name, member))

    def hash_set(self, name, mapping, **kwargs):
        return self.__redis.hset(name, mapping=mapping, **kwargs)

    def hash_get(self, name, *keys):
        keys = list(keys) if len(keys) > 0 else self.__redis.hkeys(name)
        return self.__decode(self.__redis.hmget(name, keys))

    def hash_del(self, name, *keys):
        return self.__redis.hdel(name, *keys)

    def hash_remove(self, name, *keys):
        return self.__redis.hdel(name, *keys)

    def key_get(self, pattern='*', **kwargs):
        """
        搜索key
        """
        return self.__decode(self.__redis.keys(pattern, **kwargs))

    def key_exists(self, *names):
        """
        判断key是否存在
        """
        return self.__redis.exists(*names)

    def key_type(self, name):
        """
        查看key对应value类型
        """
        return self.__decode(self.__redis.type(name))

    def key_expiration(self, name):
        """
        查看key有效时间(秒)
        """
        return self.__redis.ttl(name)

    def key_expire(self, name, time, **kwargs):
        """
        设置key过期时间(秒)
        """
        return self.__redis.expire(name, time, **kwargs)

    def key_delete(self, *names):
        return self.__redis.delete(*names)

    @staticmethod
    def __decode(value):
        if isinstance(value, bytes):
            return value.decode('utf-8')

        if not isinstance(value, Iterable):
            return value

        lst = []
        for item in value:
            lst.append(RedisHelper.__decode(item))
        return lst


if __name__ == '__main__':
    redis_conf = {
        "host": '127.0.0.1',
        "port": 6379
    }

    with RedisHelper(**redis_conf) as redis:
        redis.string_set('key', 'value')
        print(redis.string_get('key'))
        redis.increment('counter', 2)

        redis.enqueue('test_queue', 123)
        redis.enqueue('test_queue', 456)
        print(redis.dequeue('test_queue'))
        print(redis.peek_range('test_queue', stop=1))

        redis.set_add('test_set', 1, 2, 3)
        redis.set_remove('test_set', 3)
        print(redis.set_members('test_set'))

        redis.zset_add('test_zset', {'Colin': 100, 'Robin': 90})
        redis.zset_remove('test_zset', 'Robin')
        print(redis.zset_range('test_zset', 0, 1))
        print(redis.zset_range_byscore('test_zset', 90, 100))
        print(redis.zset_score('test_zset', 'Colin'))

        redis.hash_set('test_hash', {"name": 'Colin', "age": 18})
        print(redis.hash_get('test_hash', "name", "age"))
        redis.hash_remove('test_hash', "name", "age")

        print(redis.key_get("test*"))
        print(redis.key_exists('test_queue', 'test_zset'))
        print(redis.key_type('test_zset'))
        print(redis.key_expiration('test_hash'))
        print(redis.key_expire('test_hash', 2))
        redis.key_delete("key", "test_set")
