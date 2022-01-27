import settings
from utils import moudleutil
import asyncio
import random
from valid.valid import ProxyValid
from pipeline.pipelines import SqlitePipeline
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from loguru import logger


class Manager:

    def __init__(self):
        self.settings = {}
        self._proxys = []
        for key in settings.__dict__:
            if isinstance(key, str) and not key.startswith("__"):
                self.settings[key] = settings.__dict__[key]

        self._pipeline = SqlitePipeline()
        self._start = False
        # 测试模式
        self.test_mode = False
        # fetch时间  默认86400秒
        self.fetch_time = 86400
        # valid时间  默认43200秒
        self.valid_time = 43200
        # 任务调度器
        self.scheduler = None
        # 初始化代理处理器
        self._init_proxys()
        # 验证器
        self._valid = None

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

    def run_fetch(self):

        try:
            try:
                asyncio.get_event_loop()
            except RuntimeError as e:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            tasks = [p.begin() for p in self._proxys]
            if tasks is not None and len(tasks) > 0:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(asyncio.wait(tasks))
        except Exception as e:
            print(e)

    def run_valid(self):

        try:
            try:
                asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            if self._valid is None:
                self._valid = ProxyValid(self)
            self._valid()
        except Exception as exception:
            print(exception)

    async def process_item(self, item):
        logger.debug(str(item))
        if not self.test_mode:
            await self._pipeline.process_item(item)

    async def process_valid_data(self, valid_data):
        await self._pipeline.process_valid_data(valid_data)

    def random_header(self):
        """
        随机请求头
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
        return await self._pipeline.query()

    def start(self):
        if not self._start:
            self.scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
            self.scheduler.add_job(self.run_valid, trigger="interval", seconds=self.valid_time, max_instances=1)
            self.scheduler.add_job(self.run_fetch,
                                   trigger="interval",
                                   seconds=self.fetch_time,
                                   next_run_time=datetime.now(),
                                   max_instances=1)
            self.scheduler.pause_job()
            self.scheduler.start()
            self._start = True

    def stop(self):
        if self._start:
            self.scheduler.shutdown()
            self.scheduler._start = False

    def random(self, count=1):

        """
        随机获取count数据
        :param count:
        :return:
        """

        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(self._pipeline.random(count))
        loop.run_until_complete(task)
        return task.result()

    async def delete_invalid(self, delete_list):
        await self._pipeline.delete_invalid(delete_list)

    async def update_valid(self, update_list):
        await self._pipeline.process_valid_data(update_list)





