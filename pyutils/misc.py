# -*- coding: utf-8 -*-
import datetime
import sys

reload(sys)
sys.setdefaultencoding("utf-8")


def func_name(str_comment=None):
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            print(u'(%s | %s | %s)' % (datetime.datetime.now().strftime('%m-%d %H:%M:%S'),
                                       func.__name__, str_comment))
            if not args:
                return func(**kwargs)
            else:
                return func(*args, **kwargs)

        return inner_wrapper

    return wrapper


@func_name(u'你好')
def tst(in1, *in2, **in3):
    print in1, in2, in3


if __name__ == '__main__':
    try:
        tst(1, 2, 3, a=1, b=2)
    except Exception as e:
        raise
    finally:
        pass
