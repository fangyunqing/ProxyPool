from collections.abc import MutableMapping


class ProxyItem(MutableMapping):

    _fields = ["ip", "port", "location", "isp", "anon_level", "proxy_type", "valid_time", "react"]

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
