# -*- coding: utf-8 -*-
import re


def is_float(result):
    """
    判断是否是浮点数
    Args:
        string:

    Returns:

    """
    if result.isdigit():
        level = int(result) / 10.0
    else:
        level = float(result)

    print level

def test_re(string):
    dicts = {}
    if type(dicts) is dict:
        print "成功"


if __name__ == '__main__':
    test_re("0.9")
