from proxy.abstractproxy import AbstractProxy
from lxml import etree
from item.proxyitem import ProxyItem
import asyncio


class FastProxy(AbstractProxy):

    def __init__(self, url, manager, baseurl=None):
        super().__init__(url, manager, baseurl)
        self._sleep_second = 1

    def _parse(self, text):
        selector = etree.HTML(text)
        tr_list = selector.xpath('//div[@id="list"]/table/tbody/tr')
        for tr in tr_list:
            proxy_item = ProxyItem()
            proxy_item["ip"] = str(tr.xpath('./td[@data-title="IP"]/text()')[0]).strip()
            proxy_item["port"] = str(tr.xpath('./td[@data-title="PORT"]/text()')[0]).strip()
            proxy_item["anon_level"] = str(tr.xpath('./td[@data-title="匿名度"]/text()')[0]).strip()
            proxy_item["proxy_type"] = str(tr.xpath('./td[@data-title="类型"]/text()')[0]).strip()
            proxy_item["location"] = str(tr.xpath('./td[@data-title="位置"]/text()')[0]).strip()
            yield proxy_item

        li_list = selector.xpath('//div[@id="listnav"]/ul/li')
        find = False
        next_url = ""
        for li in li_list:
            if find:
                next_url = li.xpath('./a/@href')[0]
                break

            a_active = li.xpath('./a[@class="active"]')
            if a_active:
                find = True
        if find:
            yield self.baseurl + next_url
