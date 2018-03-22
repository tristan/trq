import asyncio
import pytest

@pytest.mark.asyncio
async def test_global_connection(redis):

    from trq.connection import get_global_connection

    con = await get_global_connection()
    await con.set('a', 1)
    v = await con.get('a')
    assert v == b'1'

    r = await con.incr('seq')
    assert r == 1

    redis.pause()
    await asyncio.sleep(0.1)
    redis.start()

    r = await con.incr('seq')
    assert r == 2, "redis db isn't persistent when paused"
