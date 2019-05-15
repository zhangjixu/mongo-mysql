# -*- coding: utf-8 -*-

import time
import datetime
import sys

DATETIME_FORMAT = '%Y%m%d'


def str_ts(date):
    '''
    字符串转时间戳
    Args:
        date: 传入的字符串时间

    Returns: 返回 13 位时间戳

    '''
    try:
        t = time.strptime(date, DATETIME_FORMAT)
        return int(round(time.mktime(t) * 1000))
    except Exception as e:
        raise ValueError(e), None, sys.exc_info()[2]


def ts_str(ts):
    '''
    时间戳转字符串
    Args:
        ts: 10 位时间戳

    Returns: 返回格式化的字符串时间

    '''
    try:
        st = time.localtime(ts)
        return time.strftime('%Y-%m-%d %H:%M:%S', st)
    except Exception as e:
        raise ValueError(e), None, sys.exc_info()[2]


def calculate_time(start_time, end_time):
    '''
    计算两个日期之间相差的天数
    Args:
        start_time: 开始时间
        end_time: 结束时间

    Returns:

    '''
    try:
        start = datetime.datetime.strptime(start_time, DATETIME_FORMAT)
        end = datetime.datetime.strptime(end_time, DATETIME_FORMAT)
        return (end - start).days
    except Exception as e:
        raise ValueError(e), None, sys.exc_info()[2]


def get_day(start_time, count):
    '''
    计算加上 count 之后的日期是多少
    Args:
        start_time: 开始时间
        count: 相差天数

    Returns:

    '''
    try:
        start = datetime.datetime.strptime(start_time, DATETIME_FORMAT)
        end = start + datetime.timedelta(days=count)
        return end.strftime(DATETIME_FORMAT)
    except Exception as e:
        raise ValueError(e), None, sys.exc_info()[2]


if __name__ == '__main__':
    try:
        print get_day('20180901', 100)
        print calculate_time('20180901', '20180909')
    except Exception as e:
        print e
