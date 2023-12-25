from gevent.pywsgi import WSGIServer
from gevent import monkey 
monkey.patch_all()

import os
import time
import redis
import boto3
from flask import Flask, request
from Store import Store
import container_config
from main import main

default_file = 'main.py'
work_dir = '/proxy'

remote_db_server = boto3.client('s3', aws_access_key_id=container_config.S3_ACCESS_KEY, 
                                aws_secret_access_key=container_config.S3_SECRET_KEY, 
                                region_name=container_config.S3_REGION_NAME)

redis_log_server = redis.StrictRedis(host=container_config.REDIS_HOST,
                                 port=container_config.REDIS_PORT, db=container_config.REDIS_LOG_DB)



class Runner:
    def __init__(self):
        self.workflow = None
        self.function = None
        self.bundling_size = None

    def init(self, workflow, function):
        print('init...')

        # update function status
        self.workflow = workflow
        self.function = function
        print('init finished...')

    def run(self, request_id, runtime, input, output, to, keys):
        # FaaSStore

        if 'bundling' in self.function:  # bundling fucntion
            phase = 2
        else:
            phase = 1
        latency = 0

        while (phase):
            if phase == 2:
                new_to = 'MEM'
            else:
                new_to = to

            store = Store(self.workflow, self.function, request_id, input, output, new_to, keys[str(
                2-phase)], runtime, redis_log_server, remote_db_server, phase)

            # run function
            start = time.time()
            main(store)
            end = time.time()
            latency += (end - start)
            phase -= 1
            
        return 1


proxy = Flask(__name__)
proxy.status = 'new'
proxy.debug = False
runner = Runner()


@proxy.route('/status', methods=['GET'])
def status():
    res = {}
    res['status'] = proxy.status
    res['workdir'] = os.getcwd()
    if runner.function:
        res['function'] = runner.function
    return res


@proxy.route('/init', methods=['POST'])
def init():
    proxy.status = 'init'

    inp = request.get_json(force=True, silent=True)
    runner.init(inp['workflow'], inp['function'])

    proxy.status = 'ok'
    return ('OK', 200)


@proxy.route('/run', methods=['POST'])
def run():
    proxy.status = 'run'

    inp = request.get_json(force=True, silent=True)
    request_id = inp['request_id']
    runtime = inp['runtime']
    input = inp['input']
    output = inp['output']
    to = inp['to']
    keys = inp['keys']

    # record the execution time
    start = time.time()
    if request_id.startswith('cold'):
        pass
    else:
        runner.run(request_id, runtime, input, output, to, keys)
    end = time.time()

    res = {
        "start_time": start,
        "end_time": end,
        "duration": end - start,
        "inp": inp
    }

    proxy.status = 'ok'
    return res


if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy)
    server.serve_forever()
