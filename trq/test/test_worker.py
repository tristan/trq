import asyncio
import pytest

from trq.worker import Worker
from trq.dispatch import Dispatcher
from trq.handler import BaseHandler
from trq.task import TaskError

class Handler1(BaseHandler):
    async def do_some_work(self, a, b, *, c):
        await asyncio.sleep(0.001)
        return (a, b, c)

    async def slow_task(self):
        await asyncio.sleep(1)
        return 1

class Handler2(BaseHandler):
    def initialize(self, a=None, b=None):
        self.a = a
        self.b = b

    def get_vals(self):
        return {'a': self.a, 'b': self.b}

@pytest.mark.asyncio
async def test_simple_worker(redis):

    with Worker([Handler1]):
        d = Dispatcher()
        task = d.do_some_work(1, 2, c=3)
        result = await task
        assert result == [1, 2, 3]

@pytest.mark.asyncio
async def test_handler_with_args_and_kwargs(redis):

    with Worker([(Handler2, [1], {'b': 2})]):
        d = Dispatcher()
        task = d.get_vals()
        result = await task
        assert result == {'a': 1, 'b': 2}

@pytest.mark.asyncio
async def test_handler_with_args(redis):

    with Worker([(Handler2, [1])]):
        d = Dispatcher()
        task = d.get_vals()
        result = await task
        assert result == {'a': 1, 'b': None}


@pytest.mark.asyncio
async def test_handler_with_kargs(redis):

    with Worker([(Handler2, {'b': 2})]):
        d = Dispatcher()
        task = d.get_vals()
        result = await task
        assert result == {'a': None, 'b': 2}

@pytest.mark.asyncio
async def test_workers_with_different_queues(redis):

    with Worker([(Handler1,)], queue_name="q1"), Worker([(Handler2, {'a': 1, 'b': 2})], queue_name="q2"):
        d1 = Dispatcher(queue_name="q1")
        d2 = Dispatcher(queue_name="q2")
        with pytest.raises(TaskError):
            p = await d1.get_vals()
            print(p)
        result = await d1.do_some_work(1, 2, c=3)
        assert result == [1, 2, 3]
        result = await d2.get_vals()
        assert result == {'a': 1, 'b': 2}

@pytest.mark.asyncio
async def test_redis_disconnects(redis):

    with Worker([Handler1]):
        d1 = Dispatcher()
        result = await d1.do_some_work(1, 2, c=3)
        assert result == [1, 2, 3]
        redis.pause()
        await asyncio.sleep(0.1)
        redis.start()
        result = await d1.do_some_work(1, 2, c=3)
        assert result == [1, 2, 3]

@pytest.mark.asyncio
async def test_redis_disconnects_2(redis):

    with Worker([Handler1]):
        d1 = Dispatcher()
        t = d1.slow_task()
        await asyncio.sleep(0.1)
        redis.pause()
        await asyncio.sleep(1.5)
        redis.start()
        result = await t
        assert result == 1

@pytest.mark.asyncio
async def test_redis_disconnects_3(redis):

    with Worker([Handler1]):
        d1 = Dispatcher()
        redis.pause()
        t = d1.slow_task()
        await asyncio.sleep(0.5)
        redis.start()
        result = await t
        assert result == 1

@pytest.mark.asyncio
async def test_redis_disconnects_4(redis):

    with Worker([Handler1]):
        d1 = Dispatcher()
        redis.pause()
        await asyncio.sleep(0.1)
        t = d1.slow_task()
        await asyncio.sleep(0.1)
        redis.start()
        result = await t
        assert result == 1
