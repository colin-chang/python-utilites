from oss2 import Auth, Bucket, ObjectIterator
from itertools import islice
from os.path import exists, dirname, isfile
from os import mkdir


class OssHelper(object):
    def __init__(self, **kwargs):
        auth = Auth(**kwargs.get('auth'))
        self.__bucket = Bucket(auth, **kwargs.get('bucket'))

    def upload(self, key, filename, **kwargs):
        """
        上传文件
        """
        if not exists(filename):
            raise FileNotFoundError()
        if not isfile(filename):
            raise Exception(f'{filename} must be a file')
        return self.__bucket.put_object_from_file(key, filename, **kwargs)

    def download(self, key, filename, **kwargs):
        """
        下载文件
        """
        if exists(filename):
            raise FileExistsError()

        downloads = dirname(filename)
        if len(downloads) > 0 and not exists(downloads):
            mkdir(downloads)

        return self.__bucket.get_object_to_file(key, filename, **kwargs)

    def list_objects(self, prefix='', delimiter='', marker='', max_keys=100, **kwargs):
        return islice(ObjectIterator(self.__bucket, prefix, delimiter, marker, max_keys, **kwargs), max_keys)

    def delete(self, key, **kwargs):
        """
        删除文件
        """
        return self.__bucket.delete_object(key, **kwargs)


if __name__ == '__main__':
    oss_config = {
        'auth': {
            'access_key_id': '',
            'access_key_secret': ''
        },
        'bucket': {
            'endpoint': 'https://oss-cn-beijing.aliyuncs.com',
            'bucket_name': ''
        }
    }

    oss = OssHelper(**oss_config)
    ps = {
        'key': 'dev/photos/002163ecc833e4fae5783e3a2301c18c.jpg',
        'filename': '/Users/Colin/Downloads/sample.jpg'
    }
    # oss.upload(**ps)
    # oss.download(**ps)
    objs = oss.list_objects(prefix='dev/photos', max_keys=5)
    for obj in objs:
        print(obj.key)

    # oss.delete(ps.get('key'))
