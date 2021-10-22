from collections.abc import MutableMapping


class ProxyItem(MutableMapping):
    """
    ip ip地址
    port 端口
    location 代理位置
    isp 运营商
    proxy_type 代理类型 http or https
    anon_level 匿名等级
    valid 是否验证
    valid_time 验证时间
    react 响应时间 如果验证失败 响应时间为 INVALID
    id 自增序列
    """
    _fields = ["ip", "port", "location", "isp", "proxy_type", "anon_level", "valid_time", "react"]

    def __init__(self):
        self.data = {}

    def __setitem__(self, key, value):
        if key not in self._fields:
            raise KeyError("{} is not support".format(key))

        self.data[key] = value

    def __delitem__(self, key):
        if key not in self._fields:
            raise KeyError("{} is not support".format(key))

        del self.data[key]

    def __getitem__(self, key):
        return self.data[key] if key in self.data else None

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __str__(self):
        return self.data.__str__()


if __name__ == "__main__":
    proxy_item = ProxyItem()
    print(proxy_item.items())
