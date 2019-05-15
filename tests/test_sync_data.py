# -*- coding: utf-8 -*-
import bson
import pymongo

from module.MongoUtils import MongoUtils
from module.OpsMysql import OpsMysql
from service import sync_mongo_service
from log import logger

ops_mysql = OpsMysql()


def test_sync_policy():
    # 获取 mongodb 到表的连接
    collection = MongoUtils("", "local", "oplog.rs").get_mongo_collection()
    oplog_start = bson.timestamp.Timestamp(1552666352, 6)
    oplog_end = bson.timestamp.Timestamp(1552666353, 0)
    document = {"ns": "", "ts": {"$gte": oplog_start, "$lt": oplog_end}}
    cursor = collection.find(document, cursor_type=pymongo.CursorType.TAILABLE_AWAIT, oplog_replay=True).sort(
        [(u'$natural', pymongo.ASCENDING)]).limit(100)

    for docu in cursor:
        update_policy_result(docu, "jdzz")


def update_policy_result(doc, org_id):
    """
    更新 policy_result 表数据
    Args:
        doc:
        org_id:

    Returns:

    """
    try:
        _id = doc["o2"]["_id"]
        query_sql = """ select order_id from audit_rules where `_id` = '%s'  and `org_id` = '%s'; """ % (_id, org_id)
        order_id = ops_mysql.query(query_sql)[0]["order_id"]

        if doc["o"].has_key("$set"):
            json = doc["o"]["$set"]
        else:
            json = doc["o"]

        update_sql = []

        # 判断字表是否需要更新 子表存在根据 _id org_id 删除子表 再 insert into
        if json.has_key("audit_rules"):
            audit_rules_list = json["audit_rules"]
            if type(audit_rules_list) is list and len(audit_rules_list) > 0:
                # 删除语句
                delete_sql = """ delete from `audit_rules` where _id = '%s' and `org_id` = '%s' """ % (_id, org_id)
                # 把删除语句加入更新语句中
                update_sql.append(delete_sql)

                # 子表需要保存的字段
                audit_rules_array = ["policy_en", "policy_act"]

                # 遍历循环 insert into 子表
                for audit_rules in audit_rules_list:
                    audit_rules_sql = """ insert into `audit_rules`(`_id`,`order_id`,`org_id`, """
                    audit_rules_list = []
                    audit_rules_sql_values = """ values('%s', %s, '%s', """ % (_id, order_id, org_id)
                    audit_rules_values_list = []

                    # 循环遍历拼接 slave sql 语句
                    for name in audit_rules_array:
                        if audit_rules.has_key(name) and sync_mongo_service.get_mysql_data(
                                audit_rules[name]) is not "nan" and sync_mongo_service.get_mysql_data(
                            audit_rules[name]) is not "None":
                            audit_rules_list.append("`%s`" % name)
                            audit_rules_values_list.append(
                                "'%s'" % sync_mongo_service.get_mysql_data(audit_rules[name]))

                    # 遍历完毕每一条 audit_rules 把拼接好的 sql 加入 slave_sql 中
                    sql = audit_rules_sql + "%s)" % (
                        ", ".join(audit_rules_list)) + audit_rules_sql_values + "%s);" % (
                              ", ".join(audit_rules_values_list))

                    update_sql.append(sql)

        # 主表是否需要更新
        # order_id strategy_id strategy decision 四个必须至少有一个存在
        if json.has_key("order_id") or json.has_key("strategy_id") or json.has_key(
                "strategy") or json.has_key("decision"):
            # 主表需要保存的字段
            array = ["order_id", "strategy_id", "strategy", "decision"]

            # 拼接主表 sql 语句
            policy_result_sql = """ update `policy_result` set  """
            policy_result_sql_list = []
            policy_result_sql_values = """  where `_id` = '%s'  and  `org_id` = '%s' """ % (_id, org_id)

            # 循环遍历 array 数组，拼接 master sql 语句
            for arr in array:
                if json.has_key(arr) and sync_mongo_service.get_mysql_data(
                        json[arr]) is not "nan" and sync_mongo_service.get_mysql_data(
                    json[arr]) is not "None":
                    policy_result_sql_list.append("`%s` = '%s' " % (arr, sync_mongo_service.get_mysql_data(json[arr])))

            # 拼接主 sql 语句
            master_sql = policy_result_sql + "%s" % (", ".join(policy_result_sql_list)) + policy_result_sql_values
            update_sql.append(master_sql)


    except Exception as e:
        logger.error(u" update_policy_result 函数出现异常 %s oplog 日志记录为: \n %s", e, doc)


def test_sync_model():
    # 获取 mongodb 到表的连接
    collection = MongoUtils("", "local", "oplog.rs").get_mongo_collection()
    oplog_start = bson.timestamp.Timestamp(1552618080, 11)
    oplog_end = bson.timestamp.Timestamp(1552618081, 0)
    document = {"ns": "", "ts": {"$gte": oplog_start, "$lt": oplog_end}}
    cursor = collection.find(document, cursor_type=pymongo.CursorType.TAILABLE_AWAIT, oplog_replay=True).sort(
        [(u'$natural', pymongo.ASCENDING)]).limit(100)

    for docu in cursor:
        obj = docu["o"]["model_result"]
        print obj["level"], type(obj["level"]), obj.has_key("level"), sync_mongo_service.get_mysql_data(
            obj["level"]) is not "nan", obj["level"] is not None


def update_model_result(doc, org_id):
    """
    更新 model_result 表数据
    Args:
        doc:
        org_id:

    Returns:

    """
    try:
        _id = doc["o2"]["_id"]
        if doc["o"].has_key("$set"):
            document = doc["o"]["$set"]
        else:
            document = doc["o"]

        if sync_mongo_service.is_exist(document):
            sql = """ update `model_result` set  """
            sql_values = """  where `_id`= '%s' and `org_id` = '%s' """ % (_id, org_id)

            sql_list = []

            if document.has_key("sin_id") and sync_mongo_service.get_mysql_data(
                    document["sin_id"]) is not "nan" and sync_mongo_service.get_mysql_data(
                document["sin_id"]) is not "None":
                sql_list.append("`order_id` = '%s' " % sync_mongo_service.get_mysql_data(document["sin_id"]))

            if document.has_key("model_id") and sync_mongo_service.get_mysql_data(
                    document["model_id"]) is not "nan" and sync_mongo_service.get_mysql_data(
                document["model_id"]) is not "None":
                sql_list.append("`model_id` = '%s' " % sync_mongo_service.get_mysql_data(document["model_id"]))

            if document.has_key("model_result") and type(document["model_result"]) is dict and document["model_result"]:
                obj = document["model_result"]
                str_arr = ["segment", "status_code", "default_proba"]

                for arr in str_arr:
                    if obj.has_key(arr) and sync_mongo_service.get_mysql_data(
                            obj[arr]) is not "nan" and sync_mongo_service.get_mysql_data(obj[arr]) is not "None":
                        sql_list.append("`%s` = '%s' " % (arr, sync_mongo_service.get_mysql_data(obj[arr])))

                if obj.has_key("level") and sync_mongo_service.get_mysql_data(
                        obj["level"]) is not "nan" and sync_mongo_service.get_mysql_data(
                    obj["level"]) is not "None":
                    result = sync_mongo_service.get_mysql_data(obj["level"])
                    # 如果为正整数
                    if result.isdigit():
                        level = int(result) / 10.0
                    else:
                        level = float(result)
                    sql_list.append("`lv` = '%s' " % level)

            update_sql = []
            string = sql + "%s" % (", ".join(sql_list)) + sql_values
            update_sql.append(string)


    except Exception as e:
        logger.error(u" update_model_result 函数出现异常 %s oplog 日志记录为: \n %s", e, doc)


def test():
    json = {"name": None}


if __name__ == '__main__':
    test_sync_model()
