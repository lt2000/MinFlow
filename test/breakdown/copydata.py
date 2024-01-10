import redis
import json
import pickle
import sys
import parse_yaml_min
sys.path.append('../../config')
import config

# Copy the function breakdown data for all nodes to the master node
def copy():
    # Config node IP list
    hostlist = ['172.31.42.166' ,
                '172.31.35.99' ,
                ]

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
    