import asyncio
import math
import time
import os
import msgpack
import logging
from functools import partial

from trq.connection import get_global_connection
from trq.config import get_global_config
from trq.utils import retry_on_redis_connection_error
# REDIS_CONNECTION_ERRORS, REDIS_CONNECTION_ERROR_RETRY_DELAY
from trq.task import Task

logging.basicConfig()

class Dispatcher:

    def __init__(self, *, connection=None, queue_name=None):
        self._connection = connection

        config = get_global_config()
        self._task_prefix = config['task']['prefix']
        self._seq_key = "{}seq".format(self._task_prefix)
        if queue_name is None:
            queue_name = config['queue']['name']
        self._queue_key = "{}{}".format(config['queue']['prefix'], queue_name)

        self.log = logging.getLogger('trq.dispatch:{}'.format(self.queue_name))

    @property
    def connection(self):
        return self._connection

    @retry_on_redis_connection_error
    async def _generate_task_id(self):
        with (await self.connection) as con:
            seq = await con.incr(self._seq_key)
        task_id = (math.floor(time.time()) * 1000) - 1314220021721
        task_id <<= 23
        task_id |= (os.getpid() % 8192) << 10
        task_id |= seq % 1024
        return task_id

    @retry_on_redis_connection_error
    async def _publish_task(self, task):
        with (await self.connection) as con:
            await con.set("{}{}".format(self._task_prefix, task.task_id), task.pack())
            await con.lpush(self._queue_key, task.task_id)

    @retry_on_redis_connection_error
    async def _get_result(self, task_id):
        if self.connection is None:
            self._connection = await get_global_connection()
        task_result_key = "{}{}:result".format(self._task_prefix, task_id)
        with (await self.connection) as con:
            result = await con.brpoplpush(
                task_result_key, task_result_key, timeout=0)
        if result:
            result = msgpack.unpackb(result, raw=False)
        return result

    async def _run_task(self, task):
        try:
            if self.connection is None:
                self._connection = await get_global_connection()
            task_id = await self._generate_task_id()
            task.task_id = task_id
            await self._publish_task(task)
        except Exception as e:
            self.log.exception("Error running task")
            task.set_exception(e)

    def _call_task(self, name, *args, **kwargs):
        task = Task(self, None, name, args, kwargs)
        asyncio.get_event_loop().create_task(self._run_task(task))
        return task

    def __getattr__(self, key):
        if key.startswith("_") and not hasattr(Dispatcher, key):
            # avoids calling internal methods that don't exist
            raise AttributeError("Dispatch has no attribute named '{}'".format(key))
        return partial(self._call_task, key)
