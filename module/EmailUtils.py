# -*- coding: utf-8 -*-

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from conf import email_conf
from log import email_logger


class EmailUtils(object):

    @staticmethod
    def send_email(subject_content, content, receiver):
        """
        发送邮件工具类
        Args:
            subject_content: 邮件主题
            content: 邮件正文内容
            receiver: 收件人

        Returns:

        """
        try:
            # 设置服务器
            mail_host = email_conf.mail_host
            # 用户名
            mail_user = email_conf.mail_user
            # 密码
            mail_pass = email_conf.mail_pass
            # 发件人地址
            sender = email_conf.sender
            # 邮件主题
            subject = subject_content
            # 主题包含中文时
            subject = Header(subject, 'utf-8').encode()
            # 构造邮件对象MIMEMultipart对象
            # 下面的主题，发件人，收件人，日期是显示在邮件页面上的。
            msg = MIMEMultipart('mixed')
            msg['Subject'] = subject
            msg['From'] = sender
            # 收件人为多个收件人,通过join将列表转换为以;为间隔的字符串
            msg['To'] = ";".join(receiver)
            # 构造文字内容
            text = content
            text_plain = MIMEText(text, 'plain', 'utf-8')
            msg.attach(text_plain)

            # 发送邮件
            smtp = smtplib.SMTP()
            smtp.connect(mail_host)
            # 我们用set_debuglevel(1)就可以打印出和SMTP服务器交互的所有信息。
            # smtp.set_debuglevel(1)
            smtp.login(mail_user, mail_pass)
            smtp.sendmail(sender, receiver, msg.as_string())

        except Exception as e:
            email_logger.error(" 发送邮件异常 %s ", e.message)
        finally:
            smtp.quit()
