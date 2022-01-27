import unittest
import asyncio
from proxy.enproxy import EightyNineProxy
from manager import Manager


class TestProxy(unittest.TestCase):

    def test_89proxy(self):
        m = Manager()
        m.test_mode = True
        loop = asyncio.get_event_loop()
        a = EightyNineProxy(url="https://www.89ip.cn/", manager=m)
        loop.run_until_complete(a.begin())
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
