# -*- coding: utf-8 -*-

from conf import email_conf
from module.EmailUtils import EmailUtils


def test_email():
    lists = [1, 2, 0]

    for i in lists:
        try:
            2 / i
        except Exception as e:
            EmailUtils.send_email(u"python 测试邮件", str(e), email_conf.receiver)
            break


def email():
    EmailUtils.send_email(u"nine", u"nine", email_conf.receiver)
    print "success"


if __name__ == '__main__':
    email()
