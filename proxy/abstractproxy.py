from abc import ABCMeta, abstractmethod

import aiohttp
import random
import settings
from urllib import parse
from item.proxyitem import ProxyItem
from copy import deepcopy


class AbstractProxy(metaclass=ABCMeta):
    """
    代理基类
    """

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
                        if self._manager:
                            await self._manager.process_item(p)
            finally:
                response.close()

    async def _crawl(self, url, session):
        return await session.get(url, headers=self._manager.headers())

    @abstractmethod
    def _parse(self, text):
        pass
