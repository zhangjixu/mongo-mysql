# -*- coding: utf-8 -*-

from module.OpsMysql import OpsMysql
import time
from log import logger
from conf import config
import json

ops_mysql = OpsMysql()


def innsert_mysql():
    json = {"name": ""}
    sql = """insert into `test`(`id`, `name`, `age`) values(%s, %s, %s)"""
    ops_mysql.inset_many(sql, [(1, u"张三", 18), (2, u"李四", 19)])
    print "成功"


def query_mysql():
    _id = "5c2cd808d24cff2abf58ec6d"
    org_id = "jdzz"
    query_sql = """ select order_id from audit_rules where `_id` = '%s'  and `org_id` = '%s'; """ % (_id, org_id)
    order_id = ops_mysql.query(query_sql)[0]["order_id"]
    print type(order_id), order_id


def batch_update():
    sql1 = u"""insert into `test`(`id`, `name`, `age`) values(1, '张三', 18) ;"""
    sql2 = u"""update `test` set `name` = '李四' where `id` = 1 ;"""
    sqls = [sql1, sql2]
    ops_mysql.batch_update(sqls)


def insert_many_sql():
    insert_sql = """ insert into `test`(`name`, `age`) values('wangwu', 111)"""
    update_sql = """ update `test` set `age` = 18 where `name` = 'wangwu' """
    sql = [insert_sql, update_sql]
    ops_mysql.batch_update(sql)
    print "成功"


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
        logger.error(u" 获取时间戳 sql 语句异常 %s \n sql: %s ", e, query_sql)
        result = []

    if len(result) == 0:
        with open(config.mongo_json, 'rt') as jsonFile:
            val = jsonFile.read()
            json_data = json.loads(val)
            org_json = json_data[org_id]
            result = {"ts": org_json[table_name + "_ts"], "inc": org_json[table_name + "_inc"]}
    else:
        result = result[0]

    return result



if __name__ == '__main__':
    print get_ts("jdzz", "policy_result")
