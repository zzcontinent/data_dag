# -*- coding: utf-8 -*-

import pyDes
import binascii
import traceback
import sys
import hashlib

key = '15651869'


def enc(data, ikey=key):
    k = pyDes.des(ikey, pyDes.CBC, "\0\0\0\0\0\0\0\0", pad=None, padmode=pyDes.PAD_PKCS5)
    d = k.encrypt(data)
    return bytes.decode(binascii.hexlify(d))


def dec(cipher_text, ikey=key):
    k = pyDes.des(ikey, pyDes.CBC, "\0\0\0\0\0\0\0\0", pad=None, padmode=pyDes.PAD_PKCS5)
    return bytes.decode(k.decrypt(binascii.unhexlify(cipher_text)))


# 生成签名
def md5(content):
    hash_md5 = hashlib.md5(content)
    return hash_md5.hexdigest()


#
if __name__ == '__main__':
    try:
        print(enc(sys.argv[1], key))
        print(dec(sys.argv[1], key))
        pass
    except Exception as e:
        print(e)
        print(traceback.format_exc())
    finally:
        pass
