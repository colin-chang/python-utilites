from oss2 import Auth, Bucket
from os.path import exists, dirname
from os import mkdir


class OssHelper(object):
    def __init__(self, **kwargs):
        auth = Auth(**kwargs.get('auth'))
        self.__bucket = Bucket(auth, **kwargs.get('bucket'))

    def download(self, key, dest):
        if exists(dest):
            return

        downloads = dirname(dest)
        if len(downloads) > 0 and not exists(downloads):
            mkdir(downloads)

        self.__bucket.get_object_to_file(key, dest)

    def delete(self, key):
        return self.__bucket.delete_object(key)


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
    oss.download('prod/photos/002163ecc833e4fae5783e3a2301c18c.jpg', '/Users/Colin/Downloads/sample.jpg')
    print('downloaded')

    # oss.delete('dev/photos/002163ecc833e4fae5783e3a2301c18c.jpg')
