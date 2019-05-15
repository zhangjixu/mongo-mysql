# -*- coding: utf-8 -*-

def case1(name, age):
    print name, age


def case2():
    print "hello, world"


def case3(num):
    print num


switch = {1: case1, 2: case2, 3: case3}

if __name__ == '__main__':
    switch[3](80)
