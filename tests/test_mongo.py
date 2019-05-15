# -*- coding: utf-8 -*-

import bson
import pymongo

from module.MongoUtils import MongoUtils
from service import sync_mongo_service
from log import logger


def test_mongo():
    client = MongoUtils("", "", "")
    collection = client.get_mongo_collection()
    document = {u'ut': {u'$gte': 1504022400000, u'$lt': 1504540800000}}
    # 按照 pymongo.ASCENDING 表示升序排序
    # pymongo.DESCENDING 表示降序
    cursor_ = collection.find(document, no_cursor_timeout=True).sort([(u'ut', pymongo.ASCENDING)]).limit(5)
    for docu in cursor_:
        ut = docu[u'ut']
        print ut


def add_data():
    client = MongoUtils("", "test", "test")
    collection = client.get_mongo_collection()
    students = []
    for i in range(0, 10):
        student = {"name": "name" + str(i), "age": i}
        students.append(student)

    collection.insert_many(students)


def test_query():
    collection = MongoUtils("", "", "").get_mongo_collection()

    oplog_start = bson.timestamp.Timestamp(1552298024, 0)
    oplog_end = bson.timestamp.Timestamp(1552298025, 0)
    document = {"ns": "jdzz_acrm.policy_result", "ts": {"$gte": oplog_start, "$lt": oplog_end}}

    cursor = collection.find(document, cursor_type=pymongo.CursorType.TAILABLE_AWAIT, oplog_replay=True).sort(
        [(u'$natural', pymongo.ASCENDING)]).limit(100)

    for docu in cursor:
        _id = docu["o2"]["_id"]
        s = str(_id)
        print type(_id), type(s), s
        # print type(docu["o"]["$set"]), docu["o"]["$set"]


def test_query1():
    collection = MongoUtils("", "test", "stu").get_mongo_collection()
    document = {"_id": bson.ObjectId("5cda286a7bde5ca85d3de0ea")}

    cursor = collection.find(document).limit(100)

    # for docu in cursor:
    #     # name nan age null
    #     print docu["name"], type(docu["name"]), type(docu["name"]) is float, str(docu["name"]) == "nan", str(
    #         docu["name"]).replace("nan", "zjx")
    #     print docu["age"], type(docu["age"]), docu["age"] is None
    #     print sync_mongo_service.get_mysql_data(docu["name"]) is "nan", sync_mongo_service.get_mysql_data(
    #         docu["name"]) is "None"

    for docu in cursor:
        print filter_null(docu["name"]), filter_null(docu["age"])
    cursor.close()


def test_insert_policy():
    collection = MongoUtils("", "local", "oplog.rs").get_mongo_collection()

    # 20190128 1548604800
    # 20190308 1552035860
    oplog_start = bson.timestamp.Timestamp(1552035860, 0)
    oplog_end = bson.timestamp.Timestamp(1552035861, 0)
    # 5c6e04666ffe5a3ef6a92bdc
    document = {"ns": "jdzz_acrm.policy_result", "ts": {"$gte": oplog_start, "$lt": oplog_end}}

    cursor = collection.find(document, cursor_type=pymongo.CursorType.TAILABLE_AWAIT, oplog_replay=True).sort(
        [(u'$natural', pymongo.ASCENDING)]).limit(100)

    for docu in cursor:
        try:
            string = "ss" + str(docu)
            print string
        except Exception as e:
            print e


def filter_null(value):
    """
    过滤 mongodb 中的 NaN 和 null 值
    Args:
        value:

    Returns:

    """
    flag = True
    if type(value) is float and str(value) == "nan":
        flag = False
        return flag

    if value is None:
        flag = False
        return flag
    return flag


def test():
    try:
        s = "ssss"
    except Exception as e:
        print e.message

    print s


if __name__ == '__main__':
    test_query1()
