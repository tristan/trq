import pytest
import testing.redis
import os
import signal
import aioredis

from trq.connection import set_global_connection

# adjust the defaul settings to allow unixsocket and requirepass settings
class RedisServer(testing.redis.RedisServer):

    def initialize(self):
        super().initialize()
        self.redis_conf = self.settings.get('redis_conf', {})
        if 'port' in self.redis_conf:
            port = self.redis_conf['port']
        elif self.settings['port'] is not None:
            port = self.settings['port']
        else:
            port = None
        if port == 0:
            self.redis_conf['unixsocket'] = os.path.join(self.base_dir, 'redis.sock')

    def dsn(self, **kwargs):
        params = super().dsn(**kwargs)
        if 'unixsocket' in self.redis_conf:
            del params['host']
            del params['port']
            params['unix_socket_path'] = self.redis_conf['unixsocket']
        if 'requirepass' in self.redis_conf:
            params['password'] = self.redis_conf['requirepass']
        return params

    def pause(self):
        """stops redis, without calling the cleanup"""
        self.terminate(signal.SIGTERM)

@pytest.fixture
def redis():
    from testing.redis import RedisServer
    server = RedisServer(redis_conf={
        'requirepass': 'testing',  # use password to make sure clients support using a password
        'port': 0,  # force using unix domain socket
        'loglevel': 'warning'  # suppress unnecessary messages
    })
    config = server.dsn()
    connection = aioredis.create_redis_pool(config['unix_socket_path'], password=config['password'],
                                            db=config['db'], encoding='utf-8')
    set_global_connection(connection)
    yield connection
    connection.close()
    server.stop()
