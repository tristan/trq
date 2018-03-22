import asyncio
import msgpack
import time
import traceback
import sys
import logging

from trq.config import get_global_config
from trq.connection import get_global_connection
from trq.task import TaskStatus

from trq.utils import retry_on_redis_connection_error, REDIS_CONNECTION_ERRORS, REDIS_CONNECTION_ERROR_RETRY_DELAY

logging.basicConfig()

_reserved_task_handler_functions = ['initialize']

class Worker:

    def __init__(self, handlers, *, queue_name=None, connection=None):
        self._connection = connection

        config = get_global_config()
        self.task_prefix = config['task']['prefix']
        self.task_result_expiry = int(config['task']['result_expiry'])

        self.queue_prefix = config['queue']['prefix']
        if queue_name is None:
            queue_name = config['queue']['name']
        self.queue_name = queue_name

        self.log = logging.getLogger('trq.worker:{}'.format(self.queue_name))

        self._task_handlers = {}
        for item in handlers:
            if isinstance(item, (list, tuple)):
                handler, *extras = item
                if len(extras) == 0:
                    args = None
                    kwargs = None
                elif len(extras) == 1:
                    if isinstance(extras[0], (list, tuple)):
                        args = extras[0]
                        kwargs = None
                    elif isinstance(extras[0], dict):
                        args = None
                        kwargs = extras[0]
                    else:
                        if isinstance(extras[-1], dict):
                            args = extras[:-1]
                            kwargs = extras[-1]
                        else:
                            args = extras
                            kwargs = None
                else:
                    args = extras[0]
                    kwargs = extras[1]
            else:
                handler = item
                args = None
                kwargs = None
            self.add_task_handler(handler, args=args, kwargs=kwargs)

        self._loop_task = None
        self._shutdown = False
        self._running_tasks = {}

    def add_task_handler(self, handler, args=None, kwargs=None):
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        extras = (args, kwargs)
        for fnname in dir(handler):
            if fnname.startswith('_') or fnname in _reserved_task_handler_functions:
                continue
            fn = getattr(handler, fnname)
            if not callable(fn):
                continue
            if fnname in self._task_handlers:
                raise Exception("Duplicate task handler for function: {}".format(fnname))
            self._task_handlers[fnname] = (handler, extras)

    def shutdown(self):
        """prevents new tasks from being started, and gives remaining tasks 5 seconds to complete
        returns a future which will resolve when all tasks are complete or timeout after 5 seconds"""
        self._shutdown = True
        tasks = []
        if self._loop_task:
            tasks.append(self._loop_task)
            self._loop_task = None
        else:
            f = asyncio.Future()
            f.set_result(1)
            tasks.append(f)
        for task_id, handler in self._running_tasks.items():
            tasks.append(handler)
        self.log.debug("shutdown with {} tasks".format(len(tasks)))
        return asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

    def work(self):
        self._loop_task = asyncio.get_event_loop().create_task(self._work())
        return self._loop_task

    def __enter__(self):
        self.work()
        return self

    def __exit__(self, *args, **kwargs):
        self.shutdown()

    @property
    def connection(self):
        return self._connection

    @property
    def _processing_queue_key(self):
        return "{}:processing".format(self._pending_queue_key)

    @property
    def _pending_queue_key(self):
        return "{}{}".format(self.queue_prefix, self.queue_name)

    async def _work(self):
        if self.connection is None:
            self._connection = await get_global_connection()

        while not self._shutdown:
            try:
                with (await self.connection) as con:
                    task_id = await con.brpoplpush(
                        self._pending_queue_key, self._processing_queue_key,
                        timeout=1)
                if task_id and not self._shutdown:
                    task_id = task_id.decode('utf-8')
                    self._running_tasks[task_id] = asyncio.get_event_loop().create_task(self._run_task(task_id))
            except REDIS_CONNECTION_ERRORS as e:
                await asyncio.sleep(REDIS_CONNECTION_ERROR_RETRY_DELAY)
            except asyncio.CancelledError:
                # work task was cancelled
                break
            except:
                self.log.exception("Unexpected error in work loop")

    async def _run_task(self, task_id):
        try:
            if self._shutdown:
                return
            with (await self.connection) as con:
                task = await con.get('{}{}'.format(self.task_prefix, task_id))
            task = msgpack.unpackb(task, raw=False)
            if not isinstance(task, dict) or 'name' not in task or 'start' not in task or 'created' not in task:
                await self._set_task_error(task_id, 'invalid task')
                return
            fnname = task['name']
            self.log.debug("running task {} ({})".format(task['name'], task_id))
            # handle task delay
            # TODO: this feels a bit too "dumb"
            if task['start'] > task['created']:
                delay = task['start'] - time.time()
                if delay > 0:
                    await asyncio.sleep(delay)
            if fnname not in self._task_handlers:
                self.log.warning('no task named "{}" handled by queue "{}"'.format(task['name'], self.queue_name))
                await self._set_task_error(task_id, 'no task named "{}" handled by queue "{}"'.format(task['name'], self.queue_name))
                return
            if 'args' in task:
                args = task['args']
            else:
                args = ()
            if 'kwargs' in task:
                kwargs = task['kwargs']
            else:
                kwargs = {}

            handler_class, extras = self._task_handlers[fnname]
            handler = handler_class(task_id, *extras[0], **extras[1])
            func = getattr(handler, fnname)

            result = func(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            await self._set_task_success(task_id, result)
        except asyncio.CancelledError:
            if not self._shutdown:
                raise
        except Exception as e:
            try:
                self.log.exception("task {} failed".format(task_id))
                await self._set_task_error(task_id, exc_info=sys.exc_info())
            except Exception as e2:
                self.log.exception("Unexpected error writing task result")
        finally:
            if task_id not in self._running_tasks:
                self.log.warning("missing task_id from _running_tasks")
            else:
                del self._running_tasks[task_id]

    def _set_task_error(self, task_id, error=None, exc_info=None):
        if error is None:
            error = ''
        if exc_info is not None:
            _class = exc_info[0].__name__
            error = str(exc_info[1])
            tb = ''.join(traceback.format_exception(*exc_info))
        else:
            _class = None
            tb = None
        return self._set_task_result(task_id, TaskStatus.ERROR, (_class, error, tb))

    def _set_task_success(self, task_id, result):
        return self._set_task_result(task_id, TaskStatus.SUCCESS, result)

    @retry_on_redis_connection_error
    async def _set_task_result(self, task_id, status, value):
        with (await self.connection) as con:
            pipe = con.pipeline()
            task_key = '{}{}'.format(self.task_prefix, task_id)
            result_key = '{}:result'.format(task_key)
            result = msgpack.packb({'status': status.value,
                                    'time': time.time(),
                                    'value': value},
                                   use_bin_type=True)
            pipe.lpush(result_key, result)
            pipe.expire(result_key, self.task_result_expiry)
            pipe.expire(task_key, self.task_result_expiry)
            pipe.lrem(self._processing_queue_key, 1, task_id)
            await pipe.execute()
