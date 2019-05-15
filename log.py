# -*- coding: utf-8 -*-

import os
import logging.config

cur_path = os.path.dirname(__file__)
log_conf_file = os.path.join(cur_path, 'logging.ini')
# 读取日志配置文件内容
logging.config.fileConfig(log_conf_file)

# 创建一个日志器logger
logger = logging.getLogger("root")
email_logger = logging.getLogger("email")
errorSql_logger = logging.getLogger("errorSql")
fullSql_logger = logging.getLogger("fullSql")