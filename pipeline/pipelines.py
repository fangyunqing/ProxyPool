import aiosqlite


class SqlitePipeline:

    # 数据库名称
    _database_name = "proxy_pool.db"

    # 表名
    _table_name = "proxy_pool"

    # 字段信息
    _fields = [
        {
            "name": "ip",
            "type": "TEXT",
            "unique": True,
            "nullable": False,
            "comment": "ip地址"
        },
        {
            "name": "port",
            "type": "INTEGER",
            "unique": True,
            "nullable": False,
            "comment": "端口"
        },
        {
            "name": "location",
            "type": "TEXT",
            "unique": False,
            "nullable": True,
            "comment": "归属地"
        },
        {
            "name": "isp",
            "type": "TEXT",
            "unique": False,
            "nullable": True,
            "comment": "服务商"
        },
        {
            "name": "proxy_type",
            "type": "TEXT",
            "unique": False,
            "nullable": True,
            "comment": "http or https"
        },
        {
            "name": "anon_level",
            "type": "TEXT",
            "unique": False,
            "nullable": True,
            "comment": "匿名等级"
        },
        {
            "name": "react",
            "type": "REAL",
            "unique": False,
            "nullable": True,
            "comment": "响应时间"
        },
        {
            "name": "valid_time",
            "type": "REAL",
            "unique": False,
            "nullable": True,
            "comment": "验证时间(微秒)"
        }
    ]

    async def create_table(self):

        """
        创建表
        :return: None
        """

        async def _create_table(cursor):
            field_list = list(map(lambda field: "%s %s %s" %
                                                (field["name"],
                                                 field["type"],
                                                 "NULL" if field["nullable"] else "NOT NULL"), self._fields))
            unique_list = []
            for field_u in self._fields:
                if field_u["unique"]:
                    unique_list.append(field_u["name"])
            if len(unique_list) > 0:
                field_list.append("UNIQUE(%s)" % ",".join(unique_list))
            create_sql = "create table if not exists %s (%s)" % (self._table_name, ",".join(field_list))
            await cursor.execute(create_sql)
        await self._execute(_create_table)

    def __init__(self, **kwargs):
        pass

    @staticmethod
    def _dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    async def process_item(self, item):

        """
        处理dict  插入数据
        :param item:dict
        :return: None
        """

        async def _process_item(cursor):
            port = item.get("port", None)
            ip = item.get("ip", None)

            insert = "INSERT INTO proxy_pool(ip, port, location, isp, proxy_type, anon_level, react, valid_time) " \
                     "VALUES(?, ?, ?, ?, ?, ?, ?, ?)"

            select = "SELECT 1 FROM proxy_pool WHERE ip=? and port=? "

            update = "UPDATE proxy_pool " \
                     "SET location=?, isp=?, proxy_type=?, valid_time=null, " \
                     "anon_level=?, react=null, react=?, valid_time=? " \
                     "WHERE ip=? and port=?"

            await cursor.execute(select, (ip, port))
            if await cursor.fetchone() is None:
                await cursor.execute(insert,
                                     (ip, port,
                                      item.get("location", None),
                                      item.get("isp", None),
                                      item.get("proxy_type", None),
                                      item.get("anon_level", None),
                                      item.get("react", None),
                                      item.get("valid_time", None)))
            else:
                await cursor.execute(update,
                                     (item.get("location", None),
                                      item.get("isp", None),
                                      item.get("proxy_type", None),
                                      item.get("anon_level", None),
                                      item.get("react", None),
                                      item.get("valid_time", None),
                                      ip,
                                      port))
        await self._execute(_process_item)

    async def query(self):

        """
        查询所有数据库
        :return:
        """

        async def _query(cursor):
            select = "select ip, port, location, isp, proxy_type, anon_level from proxy_pool"
            await cursor.execute(select)
            return await cursor.fetchall()
        return await self._execute(_query)

    async def process_valid_data(self, valid_data_list):

        """
        更新数据的 react和valid_data
        :param valid_data_list: list[dict]
        :return:
        """

        async def _process_valid_data(cursor):
            vd = []
            for valid_data in valid_data_list:
                vd.append((
                    valid_data.get("react", None),
                    valid_data.get("valid_time", None),
                    valid_data.get("ip", None),
                    valid_data.get("port", None)
                ))

            valid = "UPDATE proxy_pool SET react=?, valid_time=?" \
                    "WHERE ip=? and port=?"
            await cursor.executemany(valid, vd)
        await self._execute(_process_valid_data)

    async def random(self, count=1):

        """
        随机返回count条数据
        :param count:
        :return:
        """

        async def _random(cursor):
            random = "select ip, port from proxy_pool order by random() limit %d " % count
            await cursor.execute(random)
            ret = await cursor.fetchall()
            ret_len = len(ret)
            if ret_len == 1:
                return ret[0]
            elif ret_len > 1:
                return ret
        return await self._execute(_random)

    async def delete_invalid(self, invalid_list):

        """
            删除失效的数据
        :param invalid_list:
        :return:
        """

        async def _delete_invalid(cursor):
            di = []
            for invalid in invalid_list:
                di.append((
                    invalid.get("ip", None),
                    invalid.get("port", None)
                ))
            invalid = "DELETE FROM proxy_pool WHERE ip=? and port=?"
            await cursor.executemany(invalid, di)
        await self._execute(_delete_invalid)

    async def _execute(self, func):
        async with aiosqlite.connect(self._database_name) as db:
            db.row_factory = self._dict_factory
            cursor = await db.cursor()
            try:
                ret = await func(cursor)
            except Exception as e:
                await db.rollback()
                raise e
            else:
                await db.commit()
                return ret
            finally:
                await cursor.close()
