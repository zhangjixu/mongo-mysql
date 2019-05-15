# -*- coding: utf-8 -*-

import pymongo


class MongoUtils(object):
    """
    用于重复利用 mongodb 实例
    """
    db_client = {}
    collection_client = {}

    def __init__(self, url, database, collection):
        """
        初始化参数
        Args:
            url: mongodb url
            database: mongodb 数据库
            collection: mongodb 集合
        """
        self.url = 'mongodb://' + url
        self.database = database
        self.collection = collection
        self.db_key = self.url + self.database
        self.collection_key = self.db_key + self.collection
        self.client = pymongo.MongoClient(host=self.url, readPreference='secondaryPreferred')

    def get_mongo_db(self):
        """
        获取到 db 的连接
        Returns:

        """
        # 指定数据库
        if self.db_key not in self.db_client:
            db = self.client[self.database]
            self.db_client[self.db_key] = db
        else:
            db = self.dbclient[self.db_key]

        return db

    def get_mongo_collection(self):
        """
        获取到 collection 的连接
        Returns:

        """
        # 指定 collection
        if self.collection_key not in self.collection_client:
            collection = self.get_mongo_db()[self.collection]
            self.collection_client[self.collection_key] = collection
        else:
            collection = self.collection_client[self.collection_key]

        return collection
