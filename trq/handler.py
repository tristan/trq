
class BaseHandler:
    def __init__(self, task_id, *args, **kwargs):
        self._task_id = task_id
        self.initialize(*args, **kwargs)

    def initialize(*args, **kwargs):
        pass
