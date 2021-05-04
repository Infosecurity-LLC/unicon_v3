import threading
from queue import Queue


class ExceptionThread(threading.Thread):
    """
    Thread catching all exceptions from runtime and pushing it to queue, that can be listened
    """
    def __init__(self, exception_bucket: Queue, *args, **kwargs):
        super(ExceptionThread, self).__init__(*args, **kwargs)
        self.exception_bucket = exception_bucket

    def run(self) -> None:
        try:
            super(ExceptionThread, self).run()
        except Exception as e:
            self.exception_bucket.put(e)
            pass
