import aiohttp
import asyncio
import time
from utils.watch import StopWatch


class ProxyValid:
    _valid_url = "http://www.baidu.com/"

    def __init__(self, manager):
        self._manager = manager

    def valid(self):

        page_size = 100
        page_no = 1

        session = aiohttp.ClientSession()
        loop = asyncio.get_event_loop()
        try:
            while True:
                task = asyncio.ensure_future(self._manager.query(page_no, page_size))
                loop.run_until_complete(task)
                datas = task.result()
                if datas is None or len(datas) == 0:
                    break

                tasks = []
                for data in datas:
                    ip = data.get("ip", None)
                    port = data.get("port", None)

                    if ip and port:
                        tasks.append(asyncio.ensure_future(self._valid_data(ip, port, session)))

                if len(tasks) > 0:
                    loop.run_until_complete(asyncio.wait(tasks))

                page_no += 1
        finally:
            loop.run_until_complete(session.close())

    async def _valid_data(self, ip, port, session):

        valid_data = None
        try:
            sw = StopWatch()
            with sw:
                response = await session.request(method="GET",
                                                 url=self._valid_url,
                                                 proxy="http://" + ip + ":" + port,
                                                 verify_ssl=False,
                                                 timeout=6,
                                                 cookies=None)
                response.close()

            valid_data = {"ip": ip,
                          "port": port,
                          "react": sw.diff,
                          "valid_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sw.end))}

        except (asyncio.exceptions.TimeoutError, ConnectionRefusedError,
                aiohttp.ClientProxyConnectionError, aiohttp.ClientOSError,
                aiohttp.ServerDisconnectedError):
            valid_data = {"ip": ip,
                          "port": port,
                          "react": "TIMEOUT",
                          "valid_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))}
        finally:
            if valid_data is not None:
                await self._manager.process_valid_data(valid_data)
