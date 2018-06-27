import redis

from .logcenter import logger


class Client(object):

    def __init__(self, **kwargs):
        self.setting = kwargs or {
            'host': 'localhost',
            'port': 6379,
            'db': 0,
            'encoding': "utf-8",
            'decode_responses': True
        }

    def db(self):
        return redis.StrictRedis(**self.setting)

    def update(self, **kwargs):
        self.setting.update(kwargs)


def setup(**kwargs):
    global connection, client
    logger.info("setup redis db with params: {}".format(kwargs))
    if client:
        client.update(**kwargs)
    else:
        client = Client(**kwargs)
    connection = client.db()


def get_client():
    global connection
    return connection


client = Client()
connection = client.db()


__all__ = ['setup', 'get_client']
