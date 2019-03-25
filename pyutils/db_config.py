# -*- coding: utf-8 -*-

from db import CDB_MySQL, CDB_PG
import pandas as pd
from enc_dec import dec, key

# db_metadata = CDB_MySQL(conn_name='metadata', user='data_reader', passwd=dec('153db8e6e550d3a93b34547c83d19643', key),
#                         host='192.168.10.29',
#                         port=3306,
#                         db_name='metadata')


db_metadata = CDB_MySQL(conn_name='metadata', user='root', passwd=dec('b2be9c5d296a1a52', key),
                        host='localhost',
                        port=3306,
                        db_name='metadata')


class CDBMetadata:
    def __init__(self, db_metadata_engine=db_metadata.get_engine()):
        self.db_metadata_engine = db_metadata_engine
        self.df_metadata = None
        self.conn = dict()

    def __del__(self):
        self.close_conn()

    def __select_metadata(self):
        sql = '''SELECT * FROM `db_config` where is_enabled=1'''
        df = pd.read_sql(sql=sql, con=self.db_metadata_engine)
        self.df_metadata = df

    def create_conn(self, conn_name, db_type='mysql'):
        self.__select_metadata()
        df_select = self.df_metadata[self.df_metadata['conn_name'] == conn_name]
        if df_select is None:
            return
        for i, row in df_select.iterrows():
            conn_type = row['conn_type']
            ssh_sts = row['ssh_sts']
            ssh_sk = row['ssh_sk']
            env = row['env']
            conn_name = row['conn_name']
            user = row['user']
            passwd = dec(row['passwd'], key)
            host = row['host']
            port = int(row['port'])
            db_name = row['db_name']
            charset = row['charset']

            if conn_name in self.conn:
                print 'conn: {0} already created'.format(conn_name)
                return self.conn[conn_name].get_engine()
            else:
                print 'conn: {0} newly created'.format(conn_name)
            if conn_type == 0 and ssh_sts == 0:
                if db_type == 'mysql':
                    self.conn[conn_name] = CDB_MySQL(conn_name=conn_name, user=user, passwd=passwd, host=host,
                                                     port=port,
                                                     db_name=db_name,
                                                     charset=charset, echo=False)
                elif db_type == 'pg':
                    self.conn[conn_name] = CDB_PG(conn_name=conn_name, user=user, passwd=passwd, host=host,
                                                  port=port,
                                                  db_name=db_name,
                                                  charset=charset, echo=False)
                else:
                    print 'db_type must be mysql or pg'
                    return None
                return self.conn[conn_name].get_engine()
            elif conn_type == 0 and ssh_sts == 1:
                # 默认只ssh跳一次
                df_ssh = self.df_metadata[self.df_metadata['db_config_id'] == ssh_sk]
                for ii, rowSSh in df_ssh.iterrows():
                    if db_type == 'mysql':
                        self.conn[conn_name] = CDB_MySQL(
                            conn_name=conn_name,
                            user=user,
                            passwd=passwd,
                            host=host,
                            port=port,
                            db_name=db_name,
                            charset=charset,
                            echo=False,
                            ssh_need=True,
                            ssh_user=rowSSh['user'],
                            ssh_host=rowSSh['host'],
                            ssh_passwd=dec(rowSSh['passwd'], key),
                            ssh_port=int(rowSSh['port'])
                        )
                    elif db_type == 'pg':
                        self.conn[conn_name] = CDB_PG(
                            conn_name=conn_name,
                            user=user,
                            passwd=passwd,
                            host=host,
                            port=port,
                            db_name=db_name,
                            charset=charset,
                            echo=False,
                            ssh_need=True,
                            ssh_user=rowSSh['user'],
                            ssh_host=rowSSh['host'],
                            ssh_passwd=dec(rowSSh['passwd'], key),
                            ssh_port=int(rowSSh['port'])
                        )
                    else:
                        print 'db_type must be mysql or pg'
                        return None
                    return self.conn[conn_name].get_engine()

    def close_conn(self, conn_name=None):
        if conn_name is None:
            for k in self.conn.keys():
                self.conn[k].close_engine()
            self.df_metadata = None
            self.conn = dict()
        else:
            for k in self.conn.keys():
                if k == conn_name:
                    self.conn[k].close_engine()
                    self.conn.pop(k)


if __name__ == '__main__':
    try:
        dbMeta = CDBMetadata()
        conn = dbMeta.create_conn('ods_jff_information')
        conn2 = dbMeta.create_conn('ods_jff_information')
        del dbMeta
        pass
    except Exception as e:
        print(e)
        raise

    finally:
        pass

# SET NAMES utf8mb4;
# SET FOREIGN_KEY_CHECKS = 0;
#
# -- ----------------------------
# -- Table structure for db_config
# -- ----------------------------
# DROP TABLE IF EXISTS `db_config`;
# CREATE TABLE `db_config`  (
#   `db_config_id` bigint(20) NOT NULL AUTO_INCREMENT,
#   `is_enabled` int(4) NULL DEFAULT 1 COMMENT '是否有效',
#   `is_display` tinyint(255) NULL DEFAULT 0 COMMENT '是否展示: 1展示 0不展示',
#   `env` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
#   `conn_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
#   `conn_type` int(4) NULL DEFAULT NULL COMMENT '连接类型0:数据库连接,1:ssh连接',
#   `user` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
#   `passwd` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
#   `host` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
#   `port` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
#   `db_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
#   `ssh_sts` tinyint(1) NULL DEFAULT 0 COMMENT '0:不需要ssh, 1:需要ssh',
#   `ssh_sk` int(4) NULL DEFAULT NULL COMMENT 'ssh对应的配置db_config_id',
#   `charset` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT 'utf8' COMMENT '数据库编码，如utf8',
#   `created_tm` datetime(6) NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'CURRENT_TIMESTAMP(6)',
#   `created_by` bigint(20) NULL DEFAULT NULL,
#   `updated_tm` datetime(6) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(6),
#   `updated_by` bigint(20) NULL DEFAULT NULL,
#   PRIMARY KEY (`db_config_id`) USING BTREE,
#   UNIQUE INDEX `conn_name_unique`(`conn_name`) USING BTREE
# ) ENGINE = InnoDB AUTO_INCREMENT = 30 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;
#
# SET FOREIGN_KEY_CHECKS = 1;
