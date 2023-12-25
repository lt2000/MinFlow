import redis
import json
import pickle
import sys
import parse_yaml_min
sys.path.append('../../config')
import config


def copy():
    hostlist = ['172.31.34.109' ,
                '172.31.46.163' ,
                '172.31.33.210' ,
                '172.31.44.9'   ,
                '172.31.34.237' ,
                '172.31.47.234' ,
                '172.31.41.149' ,
                '172.31.45.246' ,
                '172.31.33.191' ,
                '172.31.36.98' ]

    for host in hostlist:
        r1 = redis.StrictRedis(host=host,
                               port=8002, db=1)

        r2 = redis.StrictRedis(host='172.16.0.1',
                               port=8002, db=0)
        keys = r1.keys()
        for key in keys:
            key_str = key.decode()
            data = r1.get(key_str)
            r2.set(key_str, data)


if __name__ == '__main__':
    copy()
    