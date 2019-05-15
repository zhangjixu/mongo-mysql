# -*- coding: utf-8 -*-

import datetime


def test_equal():
    string = "success"
    if "success" == string:
        print "success"


def test_dict():
    dicts = {"name": None, "age": 18, "json": {"name": "name"}}
    if type(dicts) is dict:
        print "成功"


def test_str():
    """
    字符串拼接
    Returns:

    """
    ut = 123
    _id = "abc"
    org = "jdzz"
    array = ["order_id", "strategy_id", "strategy", "decision"]

    sql = """ insert into `test`( """
    list_str = []
    for arr in array:
        list_str.append("`%s`" % arr)

    sql = sql + "%s)" % (", ".join(list_str))

    print sql


def test_arr():
    array = ["order_id", "strategy_id", "strategy", "decision"]
    if "order_isd" in array:
        print "成功"


def test_():
    name = "'lisi"
    age = 19
    string = """ insert into`test`(`name`, `age`) values('%s', %s) """ % (name.replace("'", "\\'"), age)
    print string


def get_mysql_str(inp_str):
    """
    将输入时间日期数据转为可插入mysql的数据
    :param inp_str:
    :return:
    """

    if inp_str is None:
        insert_content = 'Null'
    else:
        insert_content = "'%s'" % inp_str
    return insert_content


def format_sql_string_list_query(inp_list):
    """
    将字符串列表格式化成sql语句
    :param inp_list:
    :return:
    """
    if not isinstance(inp_list, list):
        inp_list = [inp_list]

    if len(inp_list) == 1:
        return "'%s'" % inp_list[0]
    else:
        return "', '".join(inp_list)


def get_mysql_null(inp):
    """
    将python的None转为可以插入mysql的NULL
    :param inp:
    :return:
    """
    if inp is None:
        return 'NULL'
    else:
        return inp


def get_mysql_datetime(inp_datetime, dt_format='%Y-%m-%d %H:%M:%S'):
    """
    将输入时间日期数据转为可插入mysql的数据
    :param inp_datetime:
    :param dt_format:
    :return:
    """

    if inp_datetime is None:
        insert_content = 'Null'
    else:
        insert_content = "'%s'" % inp_datetime.strftime(dt_format)
    return insert_content


def get_mysql_datetime_from_dt(inp_datetime, dt_format='%Y-%m-%d %H:%M:%S'):
    """
    将输入时间戳数据转为可插入mysql的数据
    :param inp_datetime:
    :param dt_format:
    :return:
    """
    if inp_datetime is None:
        insert_content = 'Null'
    else:
        insert_content = get_mysql_datetime(datetime.datetime(inp_datetime.year, inp_datetime.month, inp_datetime.day,
                                                              inp_datetime.hour, inp_datetime.minute,
                                                              inp_datetime.second),
                                            dt_format=dt_format)
    return insert_content


def get_mysql_int(inp_int):
    """
    将输入时间日期数据转为可插入mysql的数据
    :param inp_int:
    :return:
    """

    if inp_int is None:
        insert_content = 'Null'
    else:
        insert_content = str(int(inp_int))
    return insert_content


def get_mysql_float(inp_float, float_format=None):
    """
    将输入时间日期数据转为可插入mysql的数据
    :param inp_float:
    :param float_format:
    :return:
    """

    if inp_float is None:
        insert_content = 'Null'
    else:
        if float_format:
            insert_content = float_format % inp_float
        else:
            insert_content = str(float(inp_float))
    return insert_content


def is_exist(a, b):
    flag = False
    if a == 1:
        flag = True
        return "a"

    if a == 2:
        if b == 1:
            flag = True
            return "b"

    return "ab"


def test_if():
    asd = "s"
    if asd is not "s":
        print "成功"


if __name__ == '__main__':
    print len("0.113714470091")
    print len("0.1137144700912371")

