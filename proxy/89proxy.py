from proxy.abstractproxy import AbstractProxy

from lxml import etree
from item.proxyitem import ProxyItem


class EightyNineProxy(AbstractProxy):

    def __init__(self, url, manager, baseurl=None):
        super().__init__(url, manager, baseurl)

    def _parse(self, text):

        selector = etree.HTML(text)
        tr_list = selector.xpath('//table[@class="layui-table"]/tbody/tr')
        if len(tr_list) > 0:
            for tr in tr_list:
                proxy_item = ProxyItem()
                proxy_item["ip"] = str(tr.xpath('./td[1]/text()')[0]).strip()
                proxy_item["port"] = str(tr.xpath('./td[2]/text()')[0]).strip()
                proxy_item["location"] = str(tr.xpath('./td[3]/text()')[0]).strip()
                proxy_item["isp"] = str(tr.xpath('./td[4]/text()')[0]).strip()
                yield proxy_item

            next_url = selector.xpath('//a[@class="layui-laypage-next"]/@href')[0]
            yield self.baseurl + "/" + next_url


if __name__ == "__main__":
    import asyncio

    loop = asyncio.get_event_loop()
    a = EightyNineProxy(url="https://www.89ip.cn/", manager=None)
    loop.run_until_complete(a.begin())
