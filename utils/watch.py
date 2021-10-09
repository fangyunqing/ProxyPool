import platform
import time


class StopWatch:

    def __init__(self):
        self.begin = None
        self.end = None
        self._time = None

        os = platform.system()
        if os == "Linux":
            self._time = time.clock
        else:
            self._time = time.time

    @property
    def diff(self):
        if self.begin and self.end:
            return self.end - self.begin

    def __enter__(self):
        self.begin = self._time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = self._time()
