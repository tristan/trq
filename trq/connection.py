import aioredis
from trq.config import get_global_config

async def get_global_connection():
    global _global_connection
    if _global_connection is None:
        config = get_global_config()['redis']
        db = config.get('db', None)
        _global_connection = await aioredis.create_redis_pool(
            config['url'],
            password=config.get('password', None),
            db=int(db) if db else None)
    return _global_connection

def set_global_connection(connection):
    global _global_connection
    _global_connection = connection

_global_connection = None
