import redis
import json
import pickle
import sys
import parse_yaml_min
sys.path.append('../../config')
import config

max_pt = 0

def copy():
    hostlist = ['172.31.31.179']

    for host in hostlist:
        r1 = redis.StrictRedis(host=host,
                               port=8002, db=0)

        r2 = redis.StrictRedis(host='172.16.0.1',
                               port=8002, db=1)
        keys = r1.keys()
        for key in keys:
            key_str = key.decode()
            data = r1.get(key_str)
            r2.set(key_str, data)


def cal(hostlist, stage, request_id):
    global max_pt
    breakdown = {'input_time': {'start': [], 'end': [], 'latency': []},
                 'compute_time': {'start': [], 'end': [], 'latency': []},
                 'output_time': {'start': [], 'end': [], 'latency': []}}
    results = {}
    for host in hostlist:
        r = redis.StrictRedis(host=host, port=8002, db=0)
        keys = r.keys()
        for key in keys:
            key_str = key.decode()
            if key_str.startswith(request_id+'_' + stage):
                data = pickle.loads(r.get(key_str))
                breakdown['input_time']['start'].append(
                    data['input_time']['start'])
                breakdown['input_time']['end'].append(
                    data['input_time']['end'])
                breakdown['input_time']['latency'].append(
                    data['input_time']['latency'])
                breakdown['compute_time']['start'].append(
                    data['compute_time']['start'])
                breakdown['compute_time']['end'].append(
                    data['compute_time']['end'])
                breakdown['compute_time']['latency'].append(
                    data['compute_time']['latency'])
                breakdown['output_time']['start'].append(
                    data['output_time']['start'])
                breakdown['output_time']['end'].append(
                    data['output_time']['end'])
                breakdown['output_time']['latency'].append(
                    data['output_time']['latency'])
                
    if stage == 'min-0':
        max_pt = min(breakdown['input_time']['start'])
        
    temp = max(breakdown['input_time']['end'])
    if max_pt < temp:
        results['input_time'] = temp - max_pt
        max_pt = temp
    else:
        results['input_time'] = 0
    
    temp = max(breakdown['compute_time']['end']) 
    if temp - max_pt > 0:
         results['compute_time'] = temp - max_pt
         max_pt = temp
    else:
        results['compute_time'] = 0
    
    temp = max(breakdown['output_time']['end'])
    if temp - max_pt > 0:
         results['output_time'] = temp - max_pt
         max_pt = temp
    else:
        results['output_time'] = 0
    
         
    # results['compute_time'] = max(breakdown['compute_time']['latency'])
    # results['output_time']  = max(breakdown['output_time']['latency'])
    

    
    # print(f'stage{stage}', input_time, compute_time, output_time)
    # print(f'stage{stage}', results['input_time'], results['compute_time'], results['output_time'])
    # print(breakdown['compute_time']['latency'])

    return results


if __name__ == '__main__':
    # copy()
    workflow_name = 'tpcds-16'
    with open(workflow_name + '_' + 'request.json', 'r') as file:
        data = json.load(file)
    hostlist = ['172.16.0.1']
    split_ratio = 15
    res = {}
    for method, request_id in data.items():
        if method == 'S3' or method == 'FaaSFlow':
            shuffle_mode = 'single'
        else:
            shuffle_mode = 'min'
        shuffle_n, workflow = parse_yaml_min.parse(workflow_name, shuffle_mode)
        stages = set()
        for name in workflow.nodes.keys():
            if name.startswith('min'):
                stage_name = 'min' + '-' + name.split('-')[1]
                stages.add(stage_name)
            else:
                stages.add(name)
        stages = sorted(list(stages))
        overall_breakdown = {'In/Output Time': 0,
                             'Shuffle I/O Time': 0, 'Compute Time': 0}

        keys = list(request_id.keys())
        # print(keys)
        # request_id.pop(keys[0])
        for request, latency in request_id.items():
            k = list(latency.keys()).pop()
            split_ratio = k.split('-')[0]
            
            overall_breakdown = {'In/Output Time': 0,
                             'Shuffle I/O Time': 0, 'Compute Time': 0}
            stage_breakdown = {}
            for stage in stages:
                stage_breakdown[stage] = cal(hostlist, stage, request)
            for k, v in stage_breakdown.items():
                overall_breakdown['Compute Time'] += v['compute_time']
                if k.startswith('min'):
                    if k == 'min-0':
                        overall_breakdown['In/Output Time'] += v['input_time']
                        overall_breakdown['Shuffle I/O Time'] += v['output_time']
                    elif k == 'min-{}'.format(shuffle_n):
                        overall_breakdown['In/Output Time'] += v['output_time']
                        overall_breakdown['Shuffle I/O Time'] += v['input_time']
                    else:
                        overall_breakdown['Shuffle I/O Time'] += v['input_time']
                        overall_breakdown['Shuffle I/O Time'] += v['output_time']
                else:
                    overall_breakdown['In/Output Time'] += v['input_time']
                    overall_breakdown['In/Output Time'] += v['output_time']
            print(overall_breakdown)
            # for overall in latency.values():
            #     # overall_breakdown['Shuffle I/O Time'] = overall - (overall_breakdown['In/Output Time'] + overall_breakdown['Compute Time'])
            #     print(overall_breakdown)
    #     # times = len(request_id)
    #     # b = {key: value / times for key, value in overall_breakdown.items()}
    #     # res[method] = b
    #     # print(method, res)

    # # with open('breakdown.json', 'w') as file:
    # #     json.dump(res, file, indent=4)
