from proxy.abstractproxy import AbstractProxy

from lxml import etree
from item.proxyitem import ProxyItem


class KaiXinProxy(AbstractProxy):

    def _parse(self, text):

        selector = etree.HTML(text)
        tr_list = selector.xpath('//table[@class="active"]/tbody/tr')
        for tr in tr_list:
            proxy_item = ProxyItem()
            proxy_item["ip"] = str(tr.xpath('./td[1]/text()')[0]).strip()
            proxy_item["port"] = str(tr.xpath('./td[2]/text()')[0]).strip()
            proxy_item["anon_level"] = str(tr.xpath('./td[3]/text()')[0]).strip()
            proxy_item["proxy_type"] = str(tr.xpath('./td[4]/text()')[0]).strip()
            proxy_item["react"] = str(tr.xpath('./td[5]/text()')[0]).strip().replace("秒", "")
            proxy_item["location"] = str(tr.xpath('./td[6]/text()')[0]).strip()
            yield proxy_item

        page = selector.xpath('//div[@id="listnav"]/ul[1]/text()')
        page = [str(p).strip() for p in page]

        try:
            page_count = int("".join(page))
            page_url_list = selector.xpath('//div[@id="listnav"]/ul/li')
            if len(page_url_list) > page_count:
                hrefs = page_url_list[page_count].xpath("./a/@href")
                if len(hrefs) > 0:
                    yield self.baseurl + hrefs[0]
        except ValueError:
            pass







