# -*- coding: utf-8 -*-

import bson
import pymongo
from flask import jsonify

from api import app
from api import sync_mongo_data
from conf import email_conf
from log import *
from module.EmailUtils import EmailUtils
from module.MongoUtils import MongoUtils
from module.OpsMysql import OpsMysql

ops_mysql = OpsMysql()


@app.route('/test')
def test():
    logger.error(" logger ================= ")
    email_logger.error(" email_logger ================= ")
    errorSql_logger.error(" errorSql_logger ================= ")
    fullSql_logger.error(" fullSql_logger ================= ")

    collection = MongoUtils("", "local", "oplog.rs").get_mongo_collection()

    oplog_start = bson.timestamp.Timestamp(1552298024, 0)
    oplog_end = bson.timestamp.Timestamp(1552298025, 0)
    document = {"ns": "jdzz_acrm.policy_result", "ts": {"$gte": oplog_start, "$lt": oplog_end}}

    cursor = collection.find(document, cursor_type=pymongo.CursorType.TAILABLE_AWAIT, oplog_replay=True).sort(
        [(u'$natural', pymongo.ASCENDING)]).limit(100)

    for docu in cursor:
        _id = docu["o2"]["_id"]

    result_json = {"_id": str(_id), "ts": sync_mongo_data.get_ts("jdzz", "policy_result")}
    return jsonify(result_json)


@app.route("/email")
def test_email():
    lists = [1, 2, 0]
    for i in lists:
        try:
            2 / i
        except Exception as e:
            EmailUtils.send_email(u"测试邮件", str(e), email_conf.receiver)
            break
    return u"成功"
