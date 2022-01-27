import unittest
from manager import Manager
import time
import asyncio


class TestManager(unittest.TestCase):

    def test_manager(self):
        m = Manager()
        m.test_mode = False
        m.start()
        time.sleep(86400)

    def test_random(self):
        m = Manager()
        print(m.random())
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
