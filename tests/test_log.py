# -*- coding: utf-8 -*-
from log import logger, email_logger, errorSql_logger, fullSql_logger

if __name__ == '__main__':
    logger.error("debug message")
    email_logger.error(" 发送邮件异常 ")
    errorSql_logger.error(" 李四  errorSql_logger ")
    fullSql_logger.error(" fullSql_logger ")
