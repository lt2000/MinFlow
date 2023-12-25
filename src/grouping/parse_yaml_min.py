import yaml
import component
import sys
import os
import network
from gen_function_info import gen_function_info
sys.path.append('../../config')
import config
yaml_file_addr = config.WORKFLOW_YAML_ADDR
shuffle_mode = config.SHUFFLE_MODE


def print_workflow(nodes):
    for name in nodes:
        print('function name: ', nodes[name].name)
        print('function prev: ', nodes[name].prev)
        print('function next: ', nodes[name].next)
        # print('function nextDis: ', nodes[name].nextDis)
        # print('function source: ', nodes[name].source)
        # print('function runtime: ', nodes[name].runtime)
        print('\n====================================')
        print('====================================\n')

    pass


def parse(workflow_name):
    data = yaml.load(
        open(yaml_file_addr[workflow_name]), Loader=yaml.FullLoader)
    global_input = dict()
    start_functions = []
    nodes = dict()
    parent_cnt = dict()
    shuffle_functions = set()
    min_functions = set()
    foreach_functions = set()
    reduce_functions = set()
    bundling_functions = set()
    merge_functions = set()   # ???
    total = 0
    join_func = 'join'
    if 'global_input' in data:
        for key in data['global_input']:
            parameter = data['global_input'][key]['value']['parameter']
            global_input[parameter] = data['global_input'][key]['size']
    functions = data['functions']
    parent_cnt[functions[0]['name']] = 0     # start function
    bundling_info = {}  # Consider the case where there is only one shuffle
    min_info = {}
    foreach_info = {}
    for function in functions:
        name = function['name']
        source = function['source']
        runtime = function['runtime']
        input_files = dict()
        output_files = dict()
        next = list()
        nextDis = list()
        send_byte = 0
        if 'input' in function:
            for key in function['input']:
                input_files[key] = {'function': function['input'][key]['value']['function'],
                                    'parameter': function['input'][key]['value']['parameter'],
                                    'size': function['input'][key]['size'], 'arg': key,
                                    'type': function['input'][key]['type']}
        if 'output' in function:
            for key in function['output']:
                output_files[key] = {'size': function['output'][key]
                                     ['size'], 'type': function['output'][key]['type']}
                send_byte += function['output'][key]['size']
        send_time = send_byte / config.NETWORK_BANDWIDTH
        conditions = list()
        if 'next' in function:
            foreach_flag = False
            reducer_foreach_num = 0
            if function['next']['type'] == 'switch':
                conditions = function['next']['conditions']
            elif function['next']['type'] == 'foreach':
                foreach_flag = True
            elif function['next']['type'] == 'shuffle':
                f = function['next']['nodes'][0]
                if f not in shuffle_functions:
                    shuffle_functions.add(f)
                else:
                    function['next']['nodes'] = []
                    temp_foreach_num = net.m // net.foreach_size[-1]
                    for j in range(temp_foreach_num):
                        # function grouping and name the new function
                        function['next']['nodes'].append('min{}-'.format(f) + str(net.shuffle_n) + '-' + str(j))
                    for n in function['next']['nodes']:
                        if name in foreach_functions:
                            merge_functions.add(n)
                        if foreach_flag:
                            foreach_functions.add(n)
                        if name != 'virtual':    
                            next.append(n)
                            nextDis.append(send_time)
                            if n not in parent_cnt:
                                parent_cnt[n] = 1
                            else:
                                parent_cnt[n] = parent_cnt[n] + 1
                    current_function = component.function(name, [], next, nextDis, source, runtime,
                                          input_files, output_files, conditions)
                    if 'scale' in function:
                        current_function.set_scale(function['scale'])
                    if 'mem_usage' in function:
                        current_function.set_mem_usage(function['mem_usage'])
                    if 'split_ratio' in function:
                        current_function.set_split_ratio(function['split_ratio'])  # ???
                    total = total + 1
                    nodes[name] = current_function
                    continue                     
                net = network.min_generator(function['next']['split_ratio'])
                bundling_info['bundling_foreach_num'] = [net.m // net.foreach_size[i] for i in range(0, net.shuffle_n, 2)]
                bundling_info['split_ratio'] = net.m
                bundling_info['group_size'] = [net.group_num[0]//i for i in net.group_num]
                reducer_foreach_num = net.m // net.foreach_size[-1]
                foreach_flag = True
                if name in shuffle_functions:   # current function is a mapper
                    cur = name
                    nxt = function['next']['nodes'][0]
                    temp_foreach_num = net.m // net.foreach_size[0]
                    for j in range(temp_foreach_num):
                        next_function = []
                        next = []
                        nextDis = []

                        # function grouping and name the new function
                        name = 'min{}-'.format(cur) + str(0) + '-' + str(j)
                        min_functions.add(name)

                        # determine the successor of the new function
                        if shuffle_mode == 'min': # min shuffle
                            next_function.append(
                                'min{}-'.format(nxt) + str(1) + '-' + str(j))
                        elif shuffle_mode == 'single': # single shuffle 
                            for i in range(temp_foreach_num):
                                next_function.append(
                                    'min{}-'.format(nxt) + str(1) + '-' + str(i))
                        
                        for n in next_function:
                            if name in foreach_functions:
                                merge_functions.add(n)
                            if foreach_flag:
                                foreach_functions.add(n)
                            next.append(n)
                            nextDis.append(send_time)
                            if n not in parent_cnt:
                                parent_cnt[n] = 1
                            else:
                                parent_cnt[n] = parent_cnt[n] + 1
                        current_function = component.function(name, [], next, nextDis, source, runtime,
                                                              input_files, output_files, conditions)
                        if 'scale' in function:
                            current_function.set_scale(function['scale'])
                        if 'mem_usage' in function:
                            current_function.set_mem_usage(
                                function['mem_usage'])
                        if 'split_ratio' in function:
                            current_function.set_split_ratio(
                                net.foreach_size[0])  # set split ratio
                        total = total + 1
                        nodes[name] = current_function
                    continue             

                else:                           # next function is a mapper     
                    foreach_num = net.m // net.foreach_size[0]
                    temp_next = function['next']['nodes'][0]
                    function['next']['nodes'].clear()
                    for idx in range(foreach_num):
                            function['next']['nodes'].append(
                                'min{}-0-'.format(temp_next) + str(idx))
        
            # current function is a reducer and next function is not null
            if name in shuffle_functions and function['next']['type'] != 'shuffle':
                foreach_flag = True
                cur = name
                nxt = function['next']['nodes'][0]
                join_func = nxt
                for i in range(1, net.shuffle_n + 1):
                    temp_foreach_num = net.m // net.foreach_size[i]
                    for j in range(temp_foreach_num):
                        next_function = []
                        next = []
                        nextDis = []
                        # function grouping and name the new function
                        name = 'min{}-'.format(cur) + str(i) + '-' + str(j)
                        min_functions.add(name)
                        # determine the successor of the new function
                        if i == net.shuffle_n:
                            # n = nxt + '-' + str(j)
                            # next_function.append(n) # next fucntion is a foreach type function
                            # n = nxt + '-' + str(j)
                            next_function.append(nxt) # next fucntion is a foreach type function
                            foreach_flag = False
                        elif i % 2 == 0:
                            next_function.append(
                                    'min{}-'.format(cur) + str(i+1) + '-' + str(j))
                        else:
                            for k in range(net.m // net.foreach_size[i+1]):
                                next_function.append(
                                    'min{}-'.format(cur) + str(i+1) + '-' + str(k))

                        for n in next_function:
                            if name in foreach_functions:
                                merge_functions.add(n)
                            if foreach_flag:
                                foreach_functions.add(n)
                            next.append(n)
                            nextDis.append(send_time)
                            if n not in parent_cnt:
                                parent_cnt[n] = 1
                            else:
                                parent_cnt[n] = parent_cnt[n] + 1
                        current_function = component.function(name, [], next, nextDis, source, runtime,
                                                              input_files, output_files, conditions)
                        if 'scale' in function:
                            current_function.set_scale(function['scale'])
                        if 'mem_usage' in function:
                            current_function.set_mem_usage(
                                function['mem_usage'])
                        if 'split_ratio' in function:
                            current_function.set_split_ratio(
                                net.foreach_size[i])  # set split ratio
                        total = total + 1
                        nodes[name] = current_function
                continue

            if name != join_func:
                if join_func in function['next']['nodes']:
                    function['next']['nodes'] = []
                    temp_foreach_num = net.m // net.foreach_size[-1]
                    for j in range(temp_foreach_num):
                        function['next']['nodes'].append(join_func + '-' + str(j))
                    
                for n in function['next']['nodes']:
                    if name in foreach_functions:
                        merge_functions.add(n)
                    if foreach_flag:
                        foreach_functions.add(n)
                    if name != 'virtual':    
                        next.append(n)
                        nextDis.append(send_time)
                        if n not in parent_cnt:
                            parent_cnt[n] = 1
                        else:
                            parent_cnt[n] = parent_cnt[n] + 1          
            else:
                temp_foreach_num = net.m // net.foreach_size[-1]
                for j in range(temp_foreach_num):
                    new_name = name + '-' + str(j)
                    next = []
                    nextDis = []
                    for n in function['next']['nodes']:
                        if name in foreach_functions:
                            merge_functions.add(n)
                        if foreach_flag:
                            foreach_functions.add(new_name)
                        if name != 'virtual':    
                            next.append(n)
                            nextDis.append(send_time)
                            if n not in parent_cnt:
                                parent_cnt[n] = 1
                            else:
                                parent_cnt[n] = parent_cnt[n] + 1
                    current_function = component.function(new_name, [], next, nextDis, source, runtime,
                                              input_files, output_files, conditions)
                    if 'scale' in function:
                        current_function.set_scale(function['scale'])
                    if 'mem_usage' in function:
                        current_function.set_mem_usage(function['mem_usage'])
                    if 'split_ratio' in function:
                        current_function.set_split_ratio(net.foreach_size[-1])  # ???
                    total = total + 1
                    nodes[new_name] = current_function
                continue   
                    

        elif name in shuffle_functions:  # current function is a reducer and next function is null
            cur = name
            for i in range(1, net.shuffle_n + 1):
                temp_foreach_num = net.m // net.foreach_size[i]
                for j in range(temp_foreach_num):
                    next_function = []
                    next = []
                    nextDis = []

                    # function grouping and name the new function
                    name = 'min{}-'.format(cur) + str(i) + '-' + str(j)
                    min_functions.add(name)
                    # determine the successor of the new function
                    if i == net.shuffle_n:
                        next_function = []
                        if temp_foreach_num == 1:
                            reduce_functions.add(name)
                    elif i % 2 == 0:
                        next_function.append(
                                'min{}-'.format(cur) + str(i+1) + '-' + str(j))
                    else:
                        for k in range(net.m // net.foreach_size[i+1]):
                            next_function.append(
                                'min{}-'.format(cur) + str(i+1) + '-' + str(k))
                            
                    for n in next_function:
                        if name in foreach_functions:
                            merge_functions.add(n)
                        if foreach_flag:
                            foreach_functions.add(n)
                        next.append(n)
                        nextDis.append(send_time)
                        if n not in parent_cnt:
                            parent_cnt[n] = 1
                        else:
                            parent_cnt[n] = parent_cnt[n] + 1
                    current_function = component.function(name, [], next, nextDis, source, runtime,
                                                          input_files, output_files, conditions)
                    if 'scale' in function:
                        current_function.set_scale(function['scale'])
                    if 'mem_usage' in function:
                        current_function.set_mem_usage(
                            function['mem_usage'])
                    if 'split_ratio' in function:
                        current_function.set_split_ratio(
                            net.foreach_size[i])  # set split ratio
                    total = total + 1
                    nodes[name] = current_function
            continue

        if name != 'virtual':
            current_function = component.function(name, [], next, nextDis, source, runtime,
                                              input_files, output_files, conditions)
            if 'scale' in function:
                current_function.set_scale(function['scale'])
            if 'mem_usage' in function:
                current_function.set_mem_usage(function['mem_usage'])
            if 'split_ratio' in function:
                current_function.set_split_ratio(function['split_ratio'])  # ???
            total = total + 1
            nodes[name] = current_function

    for name in nodes:
        if name not in parent_cnt or parent_cnt[name] == 0:
            parent_cnt[name] = 0
            start_functions.append(name)
        for next_node in nodes[name].next:
            nodes[next_node].prev.append(name)
        if name in foreach_functions:
            foreach_info[name] = nodes[name].split_ratio

    

    config_path = config.FUNCTION_INFO_ADDRS
    for wfname, addr in config_path.items():
        gen_function_info(addr, nodes)
    return component.workflow(workflow_name, start_functions, nodes, global_input, total, parent_cnt, foreach_functions, min_functions, bundling_functions, bundling_info, foreach_info)



def gen_workflow(config_path: str, split_ratio: int):
    config_file = os.path.join(config_path, "flat_workflow.yaml")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        functions = config['functions']
        global_output = config['global_output']
        for func in functions:
            if 'split_ratio' in func:
                func['split_ratio'] = split_ratio
            if 'next' in func and 'split_ratio' in func['next']:
                 func['next']['split_ratio'] = split_ratio
        

    config_file = os.path.join(config_path, "flat_workflow.yaml")
    with open(config_file, 'w') as f:
        yaml_file = {}
        yaml_file['functions'] = functions
        yaml_file['global_output'] = global_output
        yaml.dump(yaml_file,f)



if __name__ == "__main__":
    split_ratio = 8
    config_path = config.FUNCTION_INFO_ADDRS
    for wfname, addr in config_path.items():
        gen_workflow(addr, split_ratio)
    workflow = parse('mapreduce-sort')
    print_workflow(workflow.nodes)
    # print('workflow_name: ', workflow.workflow_name)
    # print('start_functions: ', workflow.start_functions)
    # print('global_input: ', workflow.global_input)
    # print('parent_cnt: ', workflow.parent_cnt)
    # print('total: ', workflow.total)
    # print('foreach_functions: ', workflow.foreach_functions)
    # print('min_functions: ', workflow.min_functions)

