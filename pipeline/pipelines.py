import aiomysql
from abc import ABCMeta, abstractmethod


class AbstractPipeline(metaclass=ABCMeta):

    @abstractmethod
    async def open(self):
        """
        打开管道
        """
        pass

    @abstractmethod
    async def close(self):
        """
        关闭管道
        """
        pass

    @abstractmethod
    async def process_item(self):
        """
        处理item 可以保存数据库 redis 文件文件
        """
        pass

    @abstractmethod
    async def query(self, page_no, page_size):
        """
        分页查询
        """
        pass

    @abstractmethod
    async def process_valid_data(self, valid_data):
        """
        处理验证完的数据 valid_data 是一个字典 包含 ip port react valid_item
        """
        pass

    @abstractmethod
    async def count(self):
        """
        返回已经验证且响应时间是有效的个数
        """
        pass

    @abstractmethod
    async def random(self):
        """
        随机返回一条已经验证且有效的 ip 和 port
        """
        pass

    @abstractmethod
    async def delete_invalid(self):
        """
        删除无效的数据
        """
        pass

    @abstractmethod
    async def update_valid(self):
        """
        未验证修改为已验证
        """
        pass


class MysqlPipeline:
    _insert = "INSERT INTO proxy_pool(ip, port, location, isp, proxy_type, valid_time, valid, anon_level) " \
              "VALUES(%s, %s, %s, %s, %s, %s, 0, %s)"

    _select = "SELECT 1 FROM proxy_pool WHERE ip = %s and port = %s"

    _update = "UPDATE proxy_pool " \
              "SET location=%s, isp=%s, proxy_type=%s, valid_time=%s, valid=0, anon_level=%s " \
              "WHERE ip = %s and port = %s"

    _page = "SELECT port, ip FROM proxy_pool " \
            "WHERE id >= (SELECT id FROM proxy_pool WHERE valid=0 LIMIT %s,1) " \
            "AND valid=0 LIMIT %s"

    _valid = "UPDATE proxy_pool SET react=%s, valid_time=%s " \
             "WHERE ip = %s and port = %s"

    _count = "SELECT COUNT(*) count FROM proxy_pool WHERE react != 'INVALID' and valid = 1"

    _random = "SELECT * FROM proxy_pool AS t1 " \
              "JOIN (SELECT ROUND(RAND() * ((SELECT MAX(id) FROM proxy_pool)-(SELECT MIN(id) FROM proxy_pool))+" \
              "(SELECT MIN(id) FROM proxy_pool)) AS id) AS t2 " \
              "WHERE t1.id >= t2.id and valid = 1 and react <> 'INVALID'ORDER BY t1.id LIMIT 1"

    _delete_invalid = "DELETE FROM proxy_pool WHERE react = 'INVALID'"

    _update_valid = "UPDATE proxy_pool SET valid = 1 WHERE valid = 0"

    def __init__(self, **kwargs):

        self._host = kwargs.get("host", None)
        self._port = kwargs.get("port", None)
        self._db = kwargs.get("db", None)
        self._user = kwargs.get("user", None)
        self._password = kwargs.get("password", None)

        self._pool = None

    async def open(self):

        if self._pool is None:
            self._pool = await aiomysql.create_pool(
                minsize=5,
                maxsize=10,
                host=self._host,
                port=self._port,
                db=self._db,
                user=self._user,
                password=self._password,
                autocommit=False
            )

    async def close(self):

        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def process_item(self, item):

        port = item.get("port", None)
        ip = item.get("ip", None)

        if ip and port and self._pool:

            conn = await self._pool.acquire()
            try:
                cursor = await conn.cursor(aiomysql.DictCursor)
                try:

                    await cursor.execute(self._select, (ip, port))
                    res = await cursor.fetchone()
                    if res is None:
                        await cursor.execute(self._insert,
                                             (ip, port,
                                              item.get("location", None),
                                              item.get("isp", None),
                                              item.get("proxy_type", None),
                                              item.get("valid_time", None),
                                              item.get("anon_level", None)))
                    else:
                        await cursor.execute(self._update,
                                             (item.get("location", None),
                                              item.get("isp", None),
                                              item.get("proxy_type", None),
                                              item.get("valid_time", None),
                                              item.get("anon_level", None),
                                              ip,
                                              port))
                except Exception as e:
                    raise e
                else:
                    await conn.commit()
                finally:
                    await cursor.close()
            finally:
                await self._pool.release(conn)

    async def query(self, page_no, page_size):

        if self._pool:

            conn = await self._pool.acquire()
            try:
                cursor = await conn.cursor(aiomysql.DictCursor)
                try:
                    await cursor.execute(self._page,
                                         ((page_no - 1) * page_size,
                                          page_size))

                    return await cursor.fetchall()
                finally:
                    await cursor.close()
            finally:
                await self._pool.release(conn)

    async def process_valid_data(self, valid_data):

        if self._pool:
            conn = await self._pool.acquire()
            try:
                cursor = await conn.cursor(aiomysql.DictCursor)
                try:
                    await cursor.execute(self._valid,
                                         (valid_data.get("react", None),
                                          valid_data.get("valid_time", None),
                                          valid_data.get("ip", None),
                                          valid_data.get("port", None)))
                except Exception as e:
                    raise e
                else:
                    await conn.commit()
                finally:
                    await cursor.close()
            finally:
                await self._pool.release(conn)

    async def count(self):

        if self._pool:
            conn = await self._pool.acquire()
            try:
                cursor = await conn.cursor(aiomysql.DictCursor)
                try:
                    await cursor.execute(self._count)
                    return await cursor.fetchall()
                finally:
                    await cursor.close()
            finally:
                await self._pool.release(conn)

    async def random(self):

        if self._pool:
            conn = await self._pool.acquire()
            try:
                cursor = await conn.cursor(aiomysql.DictCursor)
                try:
                    await cursor.execute(self._random)
                    res = await cursor.fetchone()
                    if res is None:
                        return None
                    else:
                        return res.get("ip", None), res.get("port", None)
                finally:
                    await cursor.close()
            finally:
                await self._pool.release(conn)

    async def delete_invalid(self):

        if self._pool:
            conn = await self._pool.acquire()
            try:
                cursor = await conn.cursor(aiomysql.DictCursor)
                try:
                    await cursor.execute(self._delete_invalid)
                finally:
                    await cursor.close()
            finally:
                await self._pool.release(conn)

    async def update_valid(self):

        if self._pool:
            conn = await self._pool.acquire()
            try:
                cursor = await conn.cursor(aiomysql.DictCursor)
                try:
                    await cursor.execute(self.update_valid())
                finally:
                    await cursor.close()
            finally:
                await self._pool.release(conn)
