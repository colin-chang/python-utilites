from redis import StrictRedis


class RedisHelper(object):
    __instance = None
    __initialized = False

    @classmethod
    def singleton(cls):
        return cls.__instance

    def __new__(cls, **kwargs):
        if not cls.__instance:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    def __init__(self, **kwargs):
        """
        :param kwargs: Redis配置
        """
        if RedisHelper.__initialized:
            return

        self.__redis = StrictRedis(host=kwargs.get('host'), port=kwargs.get('port'), password=kwargs.get('password'))
        self.__frame_queue = kwargs.get('frame_queue')
        self.__chunk_queue = kwargs.get('chunk_queue')

        RedisHelper.__initialized = True

    def push(self, measurement_id, frame_json):
        """
        入队视频帧
        """
        self.__redis.rpush(self.__frame_queue.format(measurement_id), frame_json)

    def pop(self, measurement_id):
        """
        出队视频帧
        """
        return self.__redis.lpop(self.__frame_queue.format(measurement_id))


if __name__ == '__main__':
    redis_conf = {
        "host": '127.0.0.1',
        "port": 6379,
        "measurement_status": 'xiaoyang:measurement:client_sdk:measurement_status',
        "frame_queue": 'xiaoyang:measurement:client_sdk:frame_queue_{}',
        "chunk_queue": 'xiaoyang:measurement:client_sdk:chunk_queue_{}'
    }
    redis = RedisHelper(**redis_conf)

    measurement_id = 'b46e94d8-2b02-44ed-b067-c46ab206a455'
    redis.push(measurement_id, 'test_frame')
    frame = redis.pop(measurement_id).decode("utf-8")
    print(frame)
