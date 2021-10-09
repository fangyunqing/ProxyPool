import settings
import threading
from utils import moudleutil
import asyncio
import random
from valid.valid import ProxyValid


class Manager:
    _VALIDATING = "VALIDATING"
    _FETCHING = "FETCHING"
    _PENDING = "PENDING"

    def __init__(self):
        self.state = self._PENDING
        self.settings = {}
        self._pipelines = []
        self._proxys = []
        for key in settings.__dict__:
            if isinstance(key, str) and not key.startswith("__"):
                self.settings[key] = settings.__dict__[key]

        self._init_pipelines()
        self._init_proxys()
        self._lock = threading.Lock()
        self._valid = None
        self._primary_pipeline = None

    def _init_pipelines(self):
        if "PIPELINES" in self.settings:
            pipelines = self.settings["PIPELINES"]
            for pp in pipelines:
                try:
                    pipeline_info = pp.get("object", None)
                    cls = moudleutil.get_cls(pipeline_info)
                    params = pp.get("params", None)
                    o = cls(**params)
                    primary = pp.get("primary", None)
                    self._pipelines.append(o)
                    if primary:
                        o.primary = True
                    else:
                        o.primary = False
                except AttributeError:
                    pass
                except TypeError:
                    pass

            if len(self._pipelines) > 0:
                if all([not p.primary for p in self._pipelines]):
                    self._pipelines[0].primary = True

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

        for pp in self._pipelines:
            if hasattr(pp, "open"):
                tasks.append(pp.open())

        if len(tasks) > 0:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.wait(tasks))

    def _close_pipeline(self):

        tasks = []

        for pp in self._pipelines:
            if hasattr(pp, "close"):
                tasks.append(pp.close())

        if len(tasks) > 0:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.wait(tasks))

    def run_fetch(self):

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

    def run_valid(self):

        with self._lock:

            assert self.state == self._PENDING
            self.state = self._FETCHING

            self._open_pipeline()
            try:

                if self._primary_pipeline is None:
                    for p in self._pipelines:
                        if p.primary:
                            self._primary_pipeline = p

                if self._valid is None:
                    self._valid = ProxyValid(self)

                self._valid.valid()
            finally:
                self._close_pipeline()
                self.state = self._PENDING

    async def process_item(self, item):
        for pp in self._pipelines:
            if hasattr(pp, "process_item"):
                await pp.process_item(item)

    async def process_valid_data(self, valid_data):
        for pp in self._pipelines:
            if hasattr(pp, "process_valid_data"):
                await pp.process_valid_data(valid_data)

    def headers(self):

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
        if hasattr(self._primary_pipeline, "query"):
            return await self._primary_pipeline.query(page_no, page_size)


if __name__ == "__main__":
    maneger = Manager()
    maneger.run_valid()
