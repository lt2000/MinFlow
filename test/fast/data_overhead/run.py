from gevent import monkey
monkey.patch_all()
import uuid
import requests
import getopt
import sys
import os
sys.path.append('..')
sys.path.append('../../../config')
from repository import Repository
import config
import pandas as pd
import time
import json
repo = Repository()
TEST_PER_WORKFLOW = 60 * 60 * 60

def run_workflow(workflow_name, request_id):
    url = 'http://' + config.GATEWAY_ADDR + '/run'
    data = {'workflow':workflow_name, 'request_id': request_id}
    rep = requests.post(url, json=data)
    return rep.json()['latency']

def get_data_overhead(request_id):
    data_overhead = 0
    docs = repo.get_latencies(request_id, 'edge')
    for doc in docs:
        data_overhead += doc['time']
    return data_overhead

def analyze_workflow(workflow_name,method,split_ratio):
    print(f'----analyzing {workflow_name}----')
    repo.clear_couchdb_results()
    # # repo.clear_s3_results()
    # repo.clear_couchdb_workflow_latency()
    total = 0
    start = time.time()
    req = {}
    
    e2e_total = 0
    while time.time() - start <= TEST_PER_WORKFLOW and total <= 1:
        total += 1
        id = str(uuid.uuid4())
        if total == 1:
            id = 'cold-' + id
        print('----firing workflow----', id)
        e2e_latency = run_workflow(workflow_name, id)
        if total > 2:
            print('e2e latency: ', e2e_latency)
            e2e_total += e2e_latency
        else:
            print('cold start e2e latency: ', e2e_latency)
        req[id] = {f'{split_ratio}-{total-1}': e2e_latency}
    
    path = '{}_request.json'.format(workflow_name)
    if os.path.exists(path):
        with open(path, 'r') as f:
            data =  json.load(f)
        for k,v in req.items():
            data[method][k] = v
        with open(path, 'w') as f:
            json.dump(data, f, indent=4)
    else:
        data = {}
        methods = ['S3', 'FaaSFlow', 'Lambada', 'MinFlow']
        for m in methods:
            data[m] = {}
        for k,v in req.items():
            if not k.startswith('cold'):
                data[method][k] = v
        with open(path, 'w') as f:
            json.dump(data, f, indent=4)
           
    # print('E2E:', e2e_total/(total-2))

def analyze(method,split_ratio):
    # workflow_pool = ['cycles', 'epigenomics', 'genome', 'soykb', 'video', 'illgal_recognizer', 'fileprocessing', 'wordcount']
    # workflow_pool = ['cycles', 'epigenomics', 'genome', 'soykb']
    workflow_pool = ['mapreduce-sort']
    data_overhead = []
    for workflow in workflow_pool:
        analyze_workflow(workflow,method,split_ratio)
    # df = pd.DataFrame({'workflow': workflow_pool, 'data_overhead': data_overhead})
    # df.to_csv(mode + '2.csv')

if __name__ == '__main__':
    methods = ['S3', 'FaaSFlow', 'Lambada', 'MinFlow', 'MinFlow+Bundling']
    opts, args = getopt.getopt(sys.argv[1:],'',['split_ratio=', 'method='])
    for name, value in opts:
        if name == '--split_ratio':
            split_ratio = value
        elif name == '--method':
            method = value
    print('==============={}-{}==============='.format(methods[int(method)], split_ratio))
    analyze(methods[int(method)],split_ratio)