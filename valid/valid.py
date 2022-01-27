import aiohttp
import asyncio
import time
from utils.watch import StopWatch


class ProxyValid:
    _valid_url = "http://www.baidu.com/"

    def __init__(self, manager):
        self._manager = manager

    def __call__(self, *args, **kwargs):
        self._valid()

    def _valid(self):

        session = aiohttp.ClientSession()
        loop = asyncio.get_event_loop()
        try:
            task = asyncio.ensure_future(self._manager.query())
            loop.run_until_complete(task)
            datas = task.result()
            if datas is None or len(datas) == 0:
                return

            tasks = []
            for data in datas:
                ip = data.get("ip", None)
                port = data.get("port", None)

                if ip and port:
                    tasks.append(asyncio.ensure_future(self._valid_data(ip, port, session)))

            if len(tasks) > 0:
                loop.run_until_complete(asyncio.wait(tasks))

            update_list = []
            delete_list = []
            for task in tasks:
                ret = task.result()
                if ret:
                    if ret.get("pass", False):
                        delete_list.append(ret)
                    else:
                        update_list.append(ret)

        finally:
            loop.run_until_complete(session.close())

    async def _valid_data(self, ip, port, session):

        valid_data = None
        # noinspection PyBroadException
        try:
            sw = StopWatch()
            with sw:
                response = await session.request(method="GET",
                                                 url=self._valid_url,
                                                 proxy="http://" + ip + ":" + port,
                                                 verify_ssl=False,
                                                 timeout=6,
                                                 cookies=None,
                                                 headers=self._manager.random_header())
                response.close()

            valid_data = {"ip": ip,
                          "port": port,
                          "react": sw.diff,
                          "valid_time": sw.end,
                          "pass": True}
        except Exception:
            valid_data = {"ip": ip,
                          "port": port,
                          "pass": False}
        finally:
            if valid_data is not None:
                await self._manager.process_valid_data(valid_data)
