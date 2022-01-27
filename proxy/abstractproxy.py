from abc import ABCMeta, abstractmethod

import aiohttp
from urllib import parse
from item.proxyitem import ProxyItem
from copy import deepcopy
from utils.watch import StopWatch



class AbstractProxy(metaclass=ABCMeta):
    """
    代理基类
    """

    _valid_url = "http://www.baidu.com/"

    def __init__(self, url, manager, baseurl=None):

        if not url:
            raise ValueError("url must not be None or Empty")

        if not isinstance(url, (str, list)):
            raise ValueError("url must be str or list")

        if isinstance(url, list):
            self.url = deepcopy(url)
        else:
            self.url = [url]

        if baseurl is None:
            parse_res = parse.urlparse(self.url[0])
            self.baseurl = parse_res.scheme + "://" + parse_res.netloc
        else:
            self.baseurl = baseurl

        self.callbacks = []
        self._manager = manager

    async def begin(self):

        session = aiohttp.ClientSession()
        try:
            for u in self.url:
                await self._begin(u, session)
        finally:
            await session.close()

    async def _begin(self, url, session):
        response = await self._crawl(url, session)
        if response is not None:
            try:
                for p in self._parse(await response.text()):
                    if isinstance(p, str):
                        await self._begin(p, session)
                    elif isinstance(p, ProxyItem):
                        # 进行验证
                        ip = p.get("ip", None)
                        port = p.get("port", None)
                        if ip and port:
                            react, valid_time = await self._valid(ip, port, session)
                            if react and valid_time and self._manager:
                                p["react"] = react
                                p["valid_time"] = valid_time
                                await self._manager.process_item(p)

            finally:
                response.close()

    async def _crawl(self, url, session):
        return await session.get(url, headers=self._manager.random_header())

    async def _valid(self, ip, port, session):

        """
        验证 ip和port
        :param ip:
        :param port:
        :param session:
        :return:
        """

        try:
            sw = StopWatch()
            with sw:
                response = await session.request(method="GET",
                                                 url=self._valid_url,
                                                 proxy="http://" + ip + ":" + port,
                                                 ssl=False,
                                                 timeout=6,
                                                 cookies=None,
                                                 headers=self._manager.random_header())
                response.close()
        except Exception as e:
            print("XX：" + repr(e))
            return None, None
        else:
            return sw.diff, sw.end

    @abstractmethod
    def _parse(self, text):
        pass
