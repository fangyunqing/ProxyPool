import aiohttp
import asyncio
import time


class ProxyValid:
    _valid_url = "http://www.baidu.com/"

    def __init__(self, manager):
        self._manager = manager

    def valid(self):

        page_size = 100
        page_no = 1

        while True:
            task = asyncio.ensure_future(self._manager.query(page_no, page_size))
            loop = asyncio.get_event_loop()
            loop.run_until_complete(task)
            datas = task.result()
            if datas is None or len(datas) == 0:
                return

            tasks = []
            for data in datas:
                ip = data.get("ip", None)
                port = data.get("port", None)
                proxy_id = data.get("id", None)

                if ip and port:
                    tasks.append(asyncio.ensure_future(self._valid_data(proxy_id, ip, port)))

            if len(tasks) > 0:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(asyncio.wait(tasks))

            page_no += 1

    async def _valid_data(self, ip, port):

        session = aiohttp.ClientSession()
        try:
            st = time.time()
            response = await session.request(method="GET",
                                             url=self._valid_url,
                                             proxy="http://" + ip + ":" + port,
                                             verify_ssl=False,
                                             timeout=6,
                                             cookies=None)
            response.close()

            se = time.time()

            return {"ip": ip,
                    "port": port,
                    "react": se - st,
                    "valid_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(se)),
                    "usable": 1}
        except (asyncio.exceptions.TimeoutError, ConnectionRefusedError,
                aiohttp.ClientProxyConnectionError, aiohttp.ClientOSError):
            return {"ip": ip,
                    "port": port,
                    "valid_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                    "usable": 0}
        finally:
            await session.close()
