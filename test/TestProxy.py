import unittest
import asyncio
from proxy.enproxy import EightyNineProxy
from proxy.kuaiproxy import FastProxy
from manager import Manager


class TestProxy(unittest.TestCase):

    def test_89proxy(self):
        m = Manager()
        m.test_mode = True
        loop = asyncio.get_event_loop()
        a = EightyNineProxy(url="https://www.89ip.cn/", manager=m)
        loop.run_until_complete(a.begin())
        self.assertEqual(True, True)

    def test_fast_proxy(self):
        m = Manager()
        m.test_mode = True
        loop = asyncio.get_event_loop()
        a = FastProxy(url="https://www.kuaidaili.com/free/inha/", manager=m)
        loop.run_until_complete(a.begin())
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
