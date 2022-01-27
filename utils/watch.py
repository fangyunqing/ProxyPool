import platform
import time


class StopWatch:

    """
        一段程序的执行时间
    """

    def __init__(self):
        self._begin = None
        self._end = None
        self._time = None

        os = platform.system()
        if os == "Linux":
            self._time = time.clock
        else:
            self._time = time.time

    @property
    def diff(self):
        if self._begin and self._end:
            return self._end - self._begin

    @property
    def end(self):
        return self._end

    @property
    def begin(self):
        return self._begin

    def __enter__(self):
        self._begin = self._time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._end = self._time()

