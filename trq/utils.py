import asyncio
import aioredis.errors

REDIS_CONNECTION_ERRORS = (aioredis.errors.ConnectionClosedError, aioredis.errors.PoolClosedError,
                           FileNotFoundError, BrokenPipeError)
REDIS_CONNECTION_ERROR_RETRY_DELAY = 0.1

def retry_on_redis_connection_error(fn):

    async def wrapper(*args, **kwargs):
        while True:
            try:
                result = await fn(*args, **kwargs)
                return result
            except REDIS_CONNECTION_ERRORS as e:
                await asyncio.sleep(0.1)

    return wrapper
