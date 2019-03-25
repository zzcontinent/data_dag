# -*- coding: utf-8 -*-
import logging
from logging import Formatter
from logging.handlers import RotatingFileHandler
import os
import platform

_app_log = logging.getLogger()

debug_log = _app_log.debug
info_log = _app_log.info
warning_log = _app_log.warning
error_log = _app_log.error


def init_log(log_path, log_level='NOTSET'):
    i = 0
    if platform.system() == 'Windows':
        i = str(log_path).rfind('\\')
    else:
        i = str(log_path).rfind(r'/')
    if not os.path.exists(log_path[:i]):
        os.makedirs(log_path[:i])

    file_handler = RotatingFileHandler(log_path,
                                       mode='w', maxBytes=1024 * 1024 * 100,
                                       backupCount=10,
                                       encoding='utf-8')

    file_handler.setFormatter(Formatter(
        '%(asctime)s [in %(pathname)s:%(lineno)d] %(levelname)s: %(message)s '
    ))

    logging.getLogger().setLevel(log_level)
    logging.getLogger().addHandler(file_handler)


if __name__ == '__main__':
    try:
        init_log(r'c:\var\log\test\x.log')
        pass
    except Exception as e:
        print(e)
    finally:
        pass
