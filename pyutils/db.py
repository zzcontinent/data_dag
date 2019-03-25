# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
import traceback


class CDB:
    def __init__(self, conn_name,
                 user,
                 passwd,
                 host,
                 port,
                 db_name,
                 charset='utf8',
                 echo=False
                 ):
        self.conn_name = conn_name
        self.user = user
        self.passwd = passwd
        self.host = host
        self.port = port
        self.db_name = db_name
        self.charset = charset
        self.echo = echo
        self.engine = ''
        self.create_engine()

    def __del__(self):
        self.close_engine()

    def get_engine(self):
        return self.engine

    def restart_engine(self):
        self.close_engine()
        self.create_engine()

    # need subclass to rewrite......
    def close_engine(self):
        pass

    def create_engine(self):
        pass


class CDB_MySQL(CDB):
    def __init__(self, conn_name,
                 user,
                 passwd,
                 host,
                 port,
                 db_name,
                 charset='utf8',
                 echo=False,
                 ssh_need=False,
                 ssh_user=None,
                 ssh_host=None,
                 ssh_port=None,
                 ssh_passwd=None):
        # ssh tunnel needed?
        self.ssh_need = ssh_need
        self.ssh_user = ssh_user
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_passwd = ssh_passwd
        self.ssh_server = None
        CDB.__init__(self, conn_name=conn_name, user=user, passwd=passwd, host=host, port=port, db_name=db_name,
                     charset=charset, echo=echo)

    def create_engine(self):
        if self.engine:
            self.close_engine()

        if not self.ssh_need:
            self.engine = create_engine('mysql+pymysql://%s:%s@%s/%s?charset=%s' % (self.user,
                                                                                    self.passwd,
                                                                                    self.host + ':' +
                                                                                    str(self.port),
                                                                                    self.db_name,
                                                                                    self.charset
                                                                                    ), echo=self.echo)
            return self.engine
        else:
            self.ssh_server = SSHTunnelForwarder(
                (self.ssh_host, int(self.ssh_port)),  # ssh机的配置
                ssh_password=self.ssh_passwd,
                ssh_username=self.ssh_user,
                remote_bind_address=(self.host, int(self.port)))  # 目标机的配置
            # 开启ssh端口映射通道
            self.ssh_server.start()
            self.engine = create_engine('mysql+pymysql://%s:%s@%s/%s?charset=%s' % (self.user,
                                                                                    self.passwd,
                                                                                    '127.0.0.1' + ':' +
                                                                                    str(
                                                                                        self.ssh_server.local_bind_port),
                                                                                    self.db_name,
                                                                                    self.charset,
                                                                                    ), echo=self.echo)

    def __close_tunnel(self):
        if self.ssh_server:
            self.ssh_server.stop()

    def __close_conn(self):
        pass

    def close_engine(self):
        self.__close_tunnel()
        self.__close_conn()


class CDB_PG(CDB):
    def __init__(self, conn_name,
                 user,
                 passwd,
                 host,
                 port,
                 db_name,
                 charset='utf8',
                 echo=False,
                 ssh_need=False,
                 ssh_user=None,
                 ssh_host=None,
                 ssh_port=None,
                 ssh_passwd=None):
        # ssh tunnel needed?
        self.ssh_need = ssh_need
        self.ssh_user = ssh_user
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_passwd = ssh_passwd
        self.ssh_server = None
        CDB.__init__(self, conn_name=conn_name, user=user, passwd=passwd, host=host, port=port, db_name=db_name,
                     charset=charset, echo=echo)

    def create_engine(self):
        if self.engine:
            self.close_engine()

        if not self.ssh_need:
            self.engine = create_engine('postgresql+psycopg2://%s:%s@%s/%s' % (self.user,
                                                                               self.passwd,
                                                                               self.host + ':' +
                                                                               str(self.port),
                                                                               self.db_name
                                                                               ), echo=self.echo)
            return self.engine
        else:
            self.ssh_server = SSHTunnelForwarder(
                (self.ssh_host, int(self.ssh_port)),  # ssh机的配置
                ssh_password=self.ssh_passwd,
                ssh_username=self.ssh_user,
                remote_bind_address=(self.host, int(self.port)))  # 目标机的配置
            # 开启ssh端口映射通道
            self.ssh_server.start()
            self.engine = create_engine('postgresql+psycopg2://%s:%s@%s/%s' % (self.user,
                                                                               self.passwd,
                                                                               '127.0.0.1' + ':' +
                                                                               str(
                                                                                   self.ssh_server.local_bind_port),
                                                                               self.db_name
                                                                               ), echo=self.echo)

    def __close_tunnel(self):
        if self.ssh_server:
            self.ssh_server.stop()

    def __close_conn(self):
        pass

    def close_engine(self):
        self.__close_tunnel()
        self.__close_conn()


if __name__ == '__main__':
    try:
        # x = CDB_MySQL(conn_name='localtest', user='root', passwd='520020', host='localhost', port=3306,
        #               db_name='scrapy', ssh_need=True, ssh_host='192.168.165.28', ssh_user='pi', ssh_port=22,
        #               ssh_passwd='zzz520020')
        # out = x.get_engine().execute('select * from city limit 10')

        x = CDB_PG(conn_name='pg_test', user='xxx', passwd='xxx@321', host='192.168.10.29', port=5430,
                   db_name='dw', ssh_need=True, ssh_host='101.132.163.174', ssh_user='xx', ssh_port=22,
                   ssh_passwd='2qxxx')
        out = x.get_engine().execute('select * from app.hub_interview_cnt limit 10')
        for v in out:
            print v
        del (x)
        pass
    except Exception as e:
        print(e)
        print(traceback.format_exc())
    finally:
        pass
