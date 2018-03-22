import asyncio
import msgpack
import time
from enum import Enum

class TaskStatus(Enum):
    ERROR = 1
    SUCCESS = 2

class TaskError(Exception):
    def __init__(self, exc_type_name, exc_message, formatted_traceback):
        if isinstance(exc_type_name, bytes):
            exc_type_name = exc_type_name.decode('utf-8')
        if isinstance(exc_message, bytes):
            exc_message = exc_message.decode('utf-8')
        if isinstance(formatted_traceback, bytes):
            formatted_traceback = formatted_traceback.decode('utf-8')
        self.exc_type_name = exc_type_name
        self.exc_message = exc_message
        self.formatted_traceback = formatted_traceback

    def format_exception(self, with_traceback=False):
        return "{}: {}{}".format(
            self.exc_type_name, self.exc_message,
            "\n{}".format(self.formatted_traceback) if with_traceback
            else "")

    def __repr__(self):
        return self.exc_message

class Task:
    def __init__(self, dispatcher, task_id, function, args, kwargs):

        self.dispatcher = dispatcher
        self.task_id = task_id
        self._future = asyncio.Future()
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self._delay = 0

    def pack(self):
        now = int(time.time())
        return msgpack.packb({
            'task_id': self.task_id,
            'name': self.function,
            'args': self.args,
            'start': now + self._delay,
            'kwargs': self.kwargs,
            'created': now
        }, use_bin_type=True)

    def delay(self, seconds):
        self._delay = seconds
        return self

    def cancel(self):
        self._future.cancel()

    def set_result(self, result):
        if self._future.done():
            return  # TODO: ignore multiple results
        self._future.set_result(result)

    def set_exception(self, exc):
        if self._future.done():
            return  # TODO: ignore multiple results
        self._future.set_exception(exc)

    def done(self):
        return self._future.done()

    async def _wait_on_result(self):
        while not self.task_id:
            await asyncio.sleep(0.0000001)
        try:
            result = await self.dispatcher._get_result(self.task_id)
            if TaskStatus(result['status']) == TaskStatus.ERROR:
                self._future.set_exception(TaskError(*result['value']))
            else:
                self._future.set_result(result['value'])
        except Exception as e:
            self.dispatcher.log.exception("Error getting result")
            self._future.set_exception(e)

    def __await__(self):
        # only wait on the result if we really want to wait on the result!
        asyncio.get_event_loop().create_task(self._wait_on_result())
        return self._future.__await__()
