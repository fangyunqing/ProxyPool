import settings
import threading
from utils import moudleutil
import asyncio
import random
from valid.valid import ProxyValid
from concurrent.futures import ThreadPoolExecutor
import time


class Manager:
    _VALIDATING = "VALIDATING"
    _FETCHING = "FETCHING"
    _PENDING = "PENDING"

    def __init__(self):
        self.state = self._PENDING
        self.settings = {}
        self._proxys = []
        for key in settings.__dict__:
            if isinstance(key, str) and not key.startswith("__"):
                self.settings[key] = settings.__dict__[key]

        self._valid = None
        self._pipeline = None
        self._thread_pool = None
        self._valid_future = None
        self._fetch_future = None
        self._start = False

        self._init_pipeline()
        self._init_proxys()
        self._lock = threading.Lock()

    def __del__(self):
        self.end()

    def _init_pipeline(self):
        if "PIPELINE" in self.settings:
            pp = self.settings["PIPELINE"]
            try:
                pipeline_info = pp.get("object", None)
                cls = moudleutil.get_cls(pipeline_info)
                params = pp.get("params", None)
                self._pipeline = cls(**params)
            except (AttributeError, TypeError):
                pass

    def _init_proxys(self):
        if "PROXYS" in self.settings:
            proxys = self.settings["PROXYS"]
            for p in proxys:
                try:
                    proxy_info = p.get("object", None)
                    cls = moudleutil.get_cls(proxy_info)
                    if cls is not None:
                        url = p.get("url", None)
                        baseurl = p.get("baseurl", None)
                        o = cls(url=url, baseurl=baseurl, manager=self)
                        self._proxys.append(o)
                except AttributeError:
                    pass
                except TypeError:
                    pass

    def _open_pipeline(self):

        tasks = []
        if hasattr(self._pipeline, "open"):
            #    tasks.append(self._pipeline.open())
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self._pipeline.open())

    def _close_pipeline(self):

        tasks = []
        if hasattr(self._pipeline, "close"):
            tasks.append(self._pipeline.close())
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.wait(tasks))

    def run_fetch(self):

        # noinspection PyBroadException
        try:
            # 如果没有事件循环 则创建事件循环
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError as e:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            with self._lock:

                assert self.state == self._PENDING
                self.state = self._FETCHING

                self._open_pipeline()
                try:
                    tasks = [p.begin() for p in self._proxys]
                    if tasks is not None and len(tasks) > 0:
                        loop = asyncio.get_event_loop()
                        loop.run_until_complete(asyncio.wait(tasks))

                finally:
                    self._close_pipeline()
                    self.state = self._PENDING
        except Exception as e:
            print(e)

    def run_valid(self):

        # noinspection PyBroadException
        try:
            # 如果没有事件循环 则创建事件循环
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            with self._lock:
                assert self.state == self._PENDING
                self.state = self._FETCHING

                try:
                    self._open_pipeline()
                    if self._valid is None:
                        self._valid = ProxyValid(self)
                    self._valid.valid()
                finally:
                    self._close_pipeline()
                    self.state = self._PENDING
        except Exception as e:
            print(e)

    async def process_item(self, item):
        if hasattr(self._pipeline, "process_item"):
            await self._pipeline.process_item(item)

    async def process_valid_data(self, valid_data):
        if hasattr(self._pipeline, "process_valid_data"):
            await self._pipeline.process_valid_data(valid_data)

    def headers(self):
        """
        移出去
        :return:
        """
        uas = self.settings.get("USER_AGENTS", None)

        if uas:
            headers = {
                "User-Agent": random.choice(uas),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Connection": "keep-alive",
            }

            return headers

    async def query(self, page_no, page_size):
        if hasattr(self._pipeline, "query"):
            return await self._pipeline.query(page_no, page_size)

    def run(self):

        valid_time = 60 * 60
        fetch_time = 4 * 60 * 60
        start_time = time.time()
        next_valid_time = start_time + valid_time
        next_fetch_time = start_time + fetch_time

        while self._start:
            time.sleep(2)
            end_time = time.time()

            if self._valid_future is None:
                if next_fetch_time <= end_time:
                    next_valid_time += valid_time
                    self._valid_future = self._thread_pool.submit(self.run_valid)
            else:
                if self._valid_future.done():
                    self._valid_future = None

            if self._fetch_future is None:
                if next_valid_time <= end_time:
                    next_fetch_time += fetch_time
                    self._fetch_future = self._thread_pool.submit(self.run_fetch)
            else:
                if self._fetch_future.done():
                    self._fetch_future = None

    def start(self):

        if not self._start:
            if self._thread_pool is None:
                self._thread_pool = ThreadPoolExecutor(max_workers=3)

            self._start = True
            self._thread_pool.submit(self.run)

    def end(self):

        if self._start:
            self._start = False
            self._thread_pool.shutdown()

    def random(self):

        if self._lock.acquire(blocking=False):
            self._open_pipeline()
            try:
                loop = asyncio.get_event_loop()
                task = asyncio.ensure_future(self._pipeline.random())
                loop.run_until_complete(task)
                return task.result()
            finally:
                self._close_pipeline()
                self._lock.release()

    async def delete_invalid(self):

        res = self.settings.get("DELETE_INVALID", True)
        if isinstance(res, bool):
            if res:
                if hasattr(self._pipeline, "delete_invalid"):
                    await self._pipeline.delete_invalid()

    async def update_valid(self):
        await self._pipeline.update_valid()


if __name__ == "__main__":
    maneger = Manager()
    print(maneger.random())
