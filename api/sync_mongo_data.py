# -*- coding: utf-8 -*-

import json
import time

import bson
import pymongo
from flask import request, jsonify

from api import app
from conf import config, email_conf
from log import logger, errorSql_logger
from module.EmailUtils import EmailUtils
from module.MongoUtils import MongoUtils
from module.OpsMysql import OpsMysql
from service import sync_mongo_service

ops_mysql = OpsMysql()


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route("/sync_mongo_data", methods=['POST'])
def sync_mongo_data():
    """
    同步 mongodb 数据到 mysql
    Returns:

    """
    try:
        # 获取 post 请求的 json 数据
        json_data = json.loads(request.get_data())
        # mongodb 的 url
        ip = json_data['ip']

        org_id = json_data['org_id']
        # 过滤 oplog.rs 数据
        ns = json_data['ns']
        # 表
        table_name = ns.split(".")[1]

        # 获取 mongodb 到表的连接
        collection = MongoUtils(ip, "local", "oplog.rs").get_mongo_collection()

        # 获取初始时间戳参数
        result = get_ts(org_id, table_name)
        ts = result["ts"]
        inc = result["inc"]

        while True:
            oplog_start = bson.timestamp.Timestamp(ts, inc)
            document = {"ns": ns, "ts": {"$gt": oplog_start}}
            cursor = collection.find(document, cursor_type=pymongo.CursorType.TAILABLE_AWAIT, oplog_replay=True).sort(
                [(u'$natural', pymongo.ASCENDING)]).limit(100)

            # 循环遍历游标数据
            for docu in cursor:
                try:
                    # 解析游标数据
                    sync_mongo_service.sync_mongo(docu, org_id, table_name)
                    bts = docu["ts"]
                    # 当前时间戳
                    current_ts = bts.time
                    # 当前增量
                    current_inc = bts.inc
                    ts = current_ts
                    inc = current_inc

                    # 更新当时的 时间戳 增量
                    update_ts(org_id + "_" + table_name, current_ts, current_inc)
                except Exception as e:
                    logger.error(u" 循环遍历 cursor 的时候出现异常 %s  doc : \n %s", e, docu)
                    EmailUtils.send_email(u"循环遍历 cursor 的时候出现异常", str(e) + "\n" + str(docu), email_conf.receiver)
                    continue
            # 让游标休息 100 ms
            time.sleep(0.01)
            cursor.close()

    except Exception as e:
        logger.error(u" 获取 mongo 端数据源报错 %s", e)
        EmailUtils.send_email(u"获取 mongo 端数据源报错", str(e), email_conf.receiver)

    return jsonify(result)


def update_ts(table_name, ts, inc):
    """
    更新时间戳
    Args:
        table_name: org + table_name
        ts:
        inc:

    Returns:

    """
    try:
        query_sql = """ select ts, inc from `time_flag` where `table_name` = '%s' """ % (table_name,)
        result = ops_mysql.query(query_sql)
        if len(result) == 0:
            result_sql = """ insert into `time_flag`(`table_name`, `ts`, `inc`) values('%s', '%s', '%s')  """ % (
                table_name, ts, inc)
        else:
            result_sql = """ update `time_flag` set `ts` = %s, `inc` = %s where `table_name` = '%s' """ % (
                ts, inc, table_name)

        ops_mysql.update(result_sql)
    except Exception as e:
        errorSql_logger.error(u" 更新时间戳 sql 语句异常 %s \n result_sql: %s ", e, result_sql)
        EmailUtils.send_email(u"更新时间戳 sql 语句异常", str(e) + result_sql, email_conf.receiver)


def get_ts(org_id, table_name):
    """
    获取时间戳
    Args:
        org_id: 项目名
        table_name: 表名

    Returns:

    """
    try:
        query_sql = """ select ts, inc from `time_flag` where `table_name` = '%s' limit 1 """ % (
            org_id + "_" + table_name,)
        result = ops_mysql.query(query_sql)
    except Exception as e:
        errorSql_logger.error(u" 获取时间戳 sql 语句异常 %s \n sql: %s ", e, query_sql)
        result = []
        EmailUtils.send_email(u"获取时间戳 sql 语句异常", str(e) + "\n" + query_sql, email_conf.receiver)

    if len(result) == 0:
        with open(config.mongo_json, 'rt') as jsonFile:
            val = jsonFile.read()
            json_data = json.loads(val)
            org_json = json_data[org_id]
            result = {"ts": org_json[table_name + "_ts"], "inc": org_json[table_name + "_inc"]}
    else:
        result = result[0]

    return result
