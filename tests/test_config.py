# -*- coding: utf-8 -*-

import json


def test():
    with open("../conf/mongo_config.json", 'rt') as jsonFile:
        val = jsonFile.read()
        messageConfig = json.loads(val)
        print messageConfig


if __name__ == '__main__':
    test()
