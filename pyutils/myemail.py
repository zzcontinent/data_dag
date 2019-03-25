# -*- coding: utf-8 -*-
import traceback
import os
import smtplib
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class MyEmail:
    def __init__(self, **config_smtp):
        self.FROM_ADDR = config_smtp['FROM_ADDR']
        self.TO_ADDR = config_smtp['TO_ADDR']
        self.PASSWORD = config_smtp['PASSWORD']
        self.SMTP_SERVER = config_smtp['SMTP_SERVER']

    def send_email(self, title, content, file_path_list=None):
        msg = MIMEMultipart()
        msg['From'] = self.FROM_ADDR
        msg['To'] = ','.join(self.TO_ADDR)
        msg['Subject'] = Header(title, 'utf-8').encode()
        msg["Accept-Language"] = "zh-CN"
        msg["Accept-Charset"] = "ISO-8859-1,utf-8"
        puretext = MIMEText(content, 'plain', 'utf-8')
        msg.attach(puretext)

        for file_path in file_path_list:
            xlsxpart = MIMEApplication(open(file_path, 'rb').read())
            # if platform.system() == 'Windows':
            #     file_name = file_path[file_path.rfind('\\') + 1:]
            # else:
            #     file_name = file_path[file_path.rfind('/') + 1:]
            xlsxpart.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
            msg.attach(xlsxpart)

        server = smtplib.SMTP_SSL(self.SMTP_SERVER)
        server.login(self.FROM_ADDR, self.PASSWORD)
        server.sendmail(self.FROM_ADDR, self.TO_ADDR, msg.as_string())
        server.quit()


if __name__ == '__main__':
    try:
        emailob = MyEmail(FROM_ADDR="zzcontinent@163.com", TO_ADDR=["zzcontinent@163.com", "520020895@qq.com"],
                          PASSWORD="zzz520020",
                          SMTP_SERVER="smtp.163.com")
        emailob.send_email(title='test ', content='test content', file_path_list=[r'/home/cliff/data/sqls/ods_jff.sql'])
    except Exception as e:
        print(e)
        print(traceback.format_exc())
    finally:
        pass
