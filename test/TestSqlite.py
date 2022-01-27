import unittest
import asyncio
from pipeline.pipelines import SqlitePipeline


class TestSqlite(unittest.TestCase):

    def test_create_table(self):
        s = SqlitePipeline()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(s.create_table())
        self.assertEqual(True, True)

    def test_process_item(self):
        s = SqlitePipeline()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(s.create_table())
        loop.run_until_complete(s.process_item(
            {
                "ip": "192.168.3.156",
                "port": 2344,
                "location": "福建莆田",
                "isp": "非法",
                "proxy_type": "HTTP",
                "anon_level": "高匿"
            }
        ))
        loop.run_until_complete(s.process_item(
            {
                "ip": "192.168.3.155",
                "port": 2342,
                "location": "福建泉州",
                "isp": "合法",
                "proxy_type": "HTTPS",
                "anon_level": "高匿"
            }
        )),
        loop.run_until_complete(s.process_item(
            {
                "ip": "192.168.3.153",
                "port": 2342,
                "location": "福建泉州",
                "isp": "合法",
                "proxy_type": "HTTPS",
                "anon_level": "高匿"
            }
        ))
        self.assertEqual(True, True)

    def test_query(self):
        s = SqlitePipeline()
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(s.query())
        loop.run_until_complete(task)
        print(task.result())
        self.assertEqual(len(task.result()) > 0, True)

    def test_process_valid_data(self):
        s = SqlitePipeline()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(s.process_valid_data([
            {
                "ip": "192.168.3.155",
                "port": 2342,
                "valid_time": 1643163735,
                "react": 1
            },
            {
                "ip": "192.168.3.156",
                "port": 2344,
                "valid_time": 1643163735,
                "react": 1.01
            }
        ]))
        self.assertEqual(True, True)

    def test_random(self):
        s = SqlitePipeline()
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(s.random())
        loop.run_until_complete(task)
        print(task.result())
        task = asyncio.ensure_future(s.random(2))
        loop.run_until_complete(task)
        print(task.result())

    def test_delete_invalid(self):
        s = SqlitePipeline()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(s.delete_invalid(
            [
                {
                    "ip": "192.168.3.153",
                    "port": "2342"
                },
                {
                    "ip": "192.168.3.154",
                    "port": "2342"
                }
            ]
        ))


if __name__ == '__main__':
    unittest.main()
