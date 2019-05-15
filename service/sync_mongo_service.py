# -*- coding: utf-8 -*-

from conf import email_conf
from log import logger
from module.EmailUtils import EmailUtils
from module.OpsMysql import OpsMysql

ops_mysql = OpsMysql()


def sync_mongo(document, org_id, table_name):
    """
    解析数据到 mysql
    Args:
        document: oplog 日志记录
        org_id:
        table_name:

    Returns:

    """
    # 数据操作类型
    op = document["op"]
    # 更新的数据
    switch[op](document, org_id, table_name)


def insert(doc, org_id, table_name):
    """
    新增数据
    Args:
        doc:
        org_id:
        table_name:

    Returns:

    """
    if "policy_result" == table_name:
        add_policy_result(doc, org_id)
    elif "model_result" == table_name:
        add_model_result(doc, org_id)


def update(doc, org_id, table_name):
    """
    更新数据
    Args:
        doc:
        org_id:
        table_name:

    Returns:

    """
    if "policy_result" == table_name:
        update_policy_result(doc, org_id)
    elif "model_result" == table_name:
        update_model_result(doc, org_id)


def delete(doc, org_id, table_name):
    """
    删除数据
    Args:
        doc:
        org_id:
        table_name:

    Returns:

    """
    if "policy_result" == table_name:
        _id = doc["o"]["_id"]
        master_sql = """ delete from `policy_result` where _id = '%s' and `org_id` = '%s' """ % (_id, org_id)
        slave_sql = """ delete from `audit_rules` where _id = '%s' and `org_id` = '%s' """ % (_id, org_id)
        sql = [master_sql, slave_sql]
        ops_mysql.batch_update(sql)
    elif "model_result" == table_name:
        _id = doc["o"]["_id"]
        delete_sql = """ delete from `model_result` where _id = '%s' and `org_id` = '%s' """ % (_id, org_id)
        ops_mysql.batch_update([delete_sql])


switch = {"i": insert, "u": update, "d": delete}


def get_mysql_data(data):
    """
    转换为 mysql 可以接受的数据类型
    存在 ' 替换
    Args:
        data:

    Returns:

    """
    result = str(data).replace("'", "\\'")
    return result


def add_policy_result(doc, org_id):
    """
    新增 policy_result 表数据
    Args:
        doc:
        org_id:

    Returns:

    """
    document = doc["o"]
    _id = document["_id"]
    ut = document["ut"]
    try:
        # order_id strategy_id strategy decision 四个必须至少有一个存在
        if document.has_key("order_id") or document.has_key("strategy_id") or document.has_key(
                "strategy") or document.has_key("decision"):
            # 主表需要保存的字段
            array = ["order_id", "strategy_id", "strategy", "decision"]

            # 拼接主表 sql 语句
            policy_result_sql = """ insert into `policy_result`(`_id`,`ut`,`org_id`, """
            policy_result_sql_list = []
            policy_result_sql_values = """ values('%s', %s, '%s', """ % (_id, ut, org_id)
            policy_result_sql_values_list = []

            # 循环遍历 array 数组，拼接 master sql 语句
            for arr in array:
                if document.has_key(arr) and filter_null(document[arr]):
                    policy_result_sql_list.append("`%s`" % arr)
                    policy_result_sql_values_list.append("'%s'" % get_mysql_data(document[arr]))

            # 最终执行的 sql 集合
            save_sql = []

            # 拼接主 sql 语句
            master_sql = policy_result_sql + "%s)" % (
                ", ".join(policy_result_sql_list)) + policy_result_sql_values + "%s);" % (
                             ", ".join(policy_result_sql_values_list))

            save_sql.append(master_sql)

            order_id = get_mysql_data(document["order_id"])
            # 获取 audit_rules 数组
            audit_rules_list = document["audit_rules"]

            # 判断 audit_rules_list 是否为可以迭代的
            if type(audit_rules_list) is list and len(audit_rules_list) > 0:
                # 子表需要保存的字段
                audit_rules_array = ["policy_en", "policy_act"]

                # 循环遍历 audit_rules_list
                for audit_rules in audit_rules_list:
                    audit_rules_sql = """ insert into `audit_rules`(`_id`,`order_id`,`org_id`, """
                    audit_rules_list = []
                    audit_rules_sql_values = """ values('%s', %s, '%s', """ % (_id, order_id, org_id)
                    audit_rules_values_list = []

                    # 循环遍历拼接 slave sql 语句
                    for name in audit_rules_array:
                        if audit_rules.has_key(name) and filter_null(audit_rules[name]):
                            audit_rules_list.append("`%s`" % name)
                            audit_rules_values_list.append("'%s'" % get_mysql_data(audit_rules[name]))

                    # 遍历完毕每一条 audit_rules 把拼接好的 sql 加入 slave_sql 中
                    sql = audit_rules_sql + "%s)" % (
                        ", ".join(audit_rules_list)) + audit_rules_sql_values + "%s);" % (
                              ", ".join(audit_rules_values_list))
                    save_sql.append(sql)

            if len(save_sql) > 0:
                ops_mysql.batch_update(save_sql)

    except Exception as e:
        logger.error(u" add_policy_result 函数出现异常 %s oplog 日志记录为: \n %s", e, doc)
        EmailUtils.send_email(u"add_policy_result 函数出现异常", str(e) + "\n" + str(doc), email_conf.receiver)


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
                        if audit_rules.has_key(name) and filter_null(audit_rules[name]):
                            audit_rules_list.append("`%s`" % name)
                            audit_rules_values_list.append("'%s'" % get_mysql_data(audit_rules[name]))

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
                if json.has_key(arr) and filter_null(json[arr]):
                    policy_result_sql_list.append("`%s` = '%s' " % (arr, get_mysql_data(json[arr])))

            # 拼接主 sql 语句
            master_sql = policy_result_sql + "%s" % (", ".join(policy_result_sql_list)) + policy_result_sql_values
            update_sql.append(master_sql)

        if len(update_sql) > 0:
            ops_mysql.batch_update(update_sql)
    except Exception as e:
        logger.error(u" update_policy_result 函数出现异常 %s oplog 日志记录为: \n %s", e, doc)
        EmailUtils.send_email(u"update_policy_result 函数出现异常", str(e) + "\n" + str(doc), email_conf.receiver)


def is_exist(document):
    """
    判断 model_result 中的字段是否存在
    Args:
        document:

    Returns:

    """
    flag = False
    if document.has_key("sin_id"):
        flag = True
        return flag

    if document.has_key("model_id"):
        flag = True
        return flag

    if document.has_key("model_result"):
        obj = document["model_result"]
        if obj.has_key("level"):
            flag = True
            return flag

        if obj.has_key("segment"):
            flag = True
            return flag

        if obj.has_key("status_code"):
            flag = True
            return flag

        if obj.has_key("default_proba"):
            flag = True
            return flag
    return flag


def filter_null(value):
    """
    过滤 mongodb 中的 NaN 和 null 值
    Args:
        value:

    Returns: True 不是空值  False 是空值

    """
    flag = True
    if type(value) is float and str(value) == "nan":
        flag = False
        return flag

    if value is None:
        flag = False
        return flag
    return flag


def add_model_result(doc, org_id):
    """
    新增 model_result 表数据
    Args:
        doc:
        org_id:

    Returns:

    """
    try:
        document = doc["o"]
        _id = document["_id"]
        ut = document["ut"]
        # 判断保存的六个字段是否存在
        if is_exist(document):

            sql = """ insert into `model_result`(`_id`,`ut`,`org_id`, """
            sql_values = """  values('%s', %s, '%s',  """ % (_id, ut, org_id)

            sql_list = []
            sql_values_list = []

            if document.has_key("sin_id") and filter_null(document["sin_id"]):
                sql_list.append("order_id")
                sql_values_list.append("'%s'" % get_mysql_data(document["sin_id"]))

            if document.has_key("model_id") and filter_null(document["model_id"]):
                sql_list.append("model_id")
                sql_values_list.append("'%s'" % get_mysql_data(document["model_id"]))

            if document.has_key("model_result") and type(document["model_result"]) is dict and document["model_result"]:
                obj = document["model_result"]
                str_arr = ["segment", "status_code", "default_proba"]

                for arr in str_arr:
                    if obj.has_key(arr) and filter_null(obj[arr]):
                        sql_list.append("`%s`" % arr)
                        sql_values_list.append("'%s'" % get_mysql_data(obj[arr]))

                if obj.has_key("level") and filter_null(obj["level"]):
                    result = get_mysql_data(obj["level"])
                    # 如果为正整数
                    if result.isdigit():
                        level = int(result) / 10.0
                    else:
                        level = float(result)
                    sql_list.append("`lv`")
                    sql_values_list.append("'%s'" % level)

            string = sql + "%s)" % (", ".join(sql_list)) + sql_values + "%s);" % (", ".join(sql_values_list))
            insert_sql = [string]
            if len(insert_sql) > 0:
                ops_mysql.batch_update(insert_sql)

    except Exception as e:
        logger.error(u" add_model_result 函数出现异常 %s oplog 日志记录为: \n %s", e, doc)
        EmailUtils.send_email(u"add_model_result 函数出现异常", str(e) + "\n" + str(doc), email_conf.receiver)


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

        if is_exist(document):
            sql = """ update `model_result` set  """
            sql_values = """  where `_id`= '%s' and `org_id` = '%s' """ % (_id, org_id)

            sql_list = []

            if document.has_key("sin_id") and filter_null(document["sin_id"]):
                sql_list.append("`order_id` = '%s' " % get_mysql_data(document["sin_id"]))

            if document.has_key("model_id") and filter_null(document["model_id"]):
                sql_list.append("`model_id` = '%s' " % get_mysql_data(document["model_id"]))

            if document.has_key("model_result") and type(document["model_result"]) is dict and document["model_result"]:
                obj = document["model_result"]
                str_arr = ["segment", "status_code", "default_proba"]

                for arr in str_arr:
                    if obj.has_key(arr) and filter_null(obj[arr]):
                        sql_list.append("`%s` = '%s' " % (arr, get_mysql_data(obj[arr])))

                if obj.has_key("level") and filter_null(obj["level"]):
                    result = get_mysql_data(obj["level"])
                    # 如果为正整数
                    if result.isdigit():
                        level = int(result) / 10.0
                    else:
                        level = float(result)
                    sql_list.append("`lv` = '%s' " % level)

            update_sql = []
            string = sql + "%s" % (", ".join(sql_list)) + sql_values
            update_sql.append(string)
            if len(update_sql) > 0:
                ops_mysql.batch_update(update_sql)

    except Exception as e:
        logger.error(u" update_model_result 函数出现异常 %s oplog 日志记录为: \n %s", e, doc)
        EmailUtils.send_email(u"update_model_result 函数出现异常", str(e) + "\n" + str(doc), email_conf.receiver)
