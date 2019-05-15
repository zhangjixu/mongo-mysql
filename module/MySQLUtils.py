# -*- coding: utf-8 -*-

from mysql.connector import pooling
from mysql.connector import MySQLConnection

MYSQL_POOL = {}


class MySQLUtils(object):
    def __init__(self, user, password, host, port, database, pool_size=0):
        """
        建立连接池
        :param user
        :param password:
        :param host:
        :param port:
        :param database:
        :param pool_size: 如果为0，直接建立普通短连接；如果大于0，则建立长连接的连接池
        """
        self._database = database
        self._host = host
        self._user = user
        self._port = port
        self._password = password
        self._pool_size = pool_size

        if pool_size < 0:
            raise ValueError('pool_size must >= 0')

        elif pool_size > 0:
            pool_name = pooling.generate_pool_name(user=user, host=host, port=port, database=database)
            if pool_name not in MYSQL_POOL:
                MYSQL_POOL[pool_name] = pooling.MySQLConnectionPool(pool_name=pool_name, pool_size=pool_size,
                                                                    user=user, password=password, host=host,
                                                                    port=port, database=database, autocommit=False)
            self.pool = MYSQL_POOL[pool_name]

    def __enter__(self):
        if self._pool_size == 0:
            self.cnx = MySQLConnection(user=self._user, password=self._password, host=self._host,
                                       port=self._port, database=self._database)
        else:
            self.cnx = self.pool.get_connection()
        return self.cnx

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 关闭数据库链接, 使用MySQLConnectionPool的情况下， 并不会真正关闭
        self.cnx.close()
