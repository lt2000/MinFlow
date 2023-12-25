import sys
import parse_yaml_bundling
import queue
import component
import repository
import yaml
sys.path.append('../../config')
import config

mem_usage = 0
max_mem_usage = 0
group_ip = {}
group_scale = {}


def topo_sort(workflow):
    in_degree_vec = dict()
    q = queue.Queue()

    # get in-degree array
    for name in workflow.start_functions:
        q.put(workflow.nodes[name])
        in_degree_vec[name] = 0
    while q.empty() is False:
        node = q.get()
        for next_node_name in node.next:
            if next_node_name not in in_degree_vec:
                in_degree_vec[next_node_name] = 1
                q.put(workflow.nodes[next_node_name])
            else:
                in_degree_vec[next_node_name] += 1

    # zoreInDegree stack
    zoreInDegree = []
    for k, v in in_degree_vec.items():
        if v == 0:
            zoreInDegree.append(k)
    # topo sorted list
    topo = []
    while (not (not zoreInDegree)):
        # for key, candidate in enumerate(zoreInDegree):
        #     if 'min' in candidate:
        #         func = zoreInDegree.pop(key)
        #         break
        func = zoreInDegree.pop()
        topo.append(func)
        for next_node_name in workflow.nodes[func].next:
            in_degree_vec[next_node_name] -= 1
            if in_degree_vec[next_node_name] == 0:
                zoreInDegree.append(next_node_name)
    return topo

# create new workflow


def create_workflow(part, func_list, workflow: component.workflow):
    workflow_name = workflow.workflow_name
    if part == 0:
        global_input = workflow.global_input
    else:
        global_input = {}

    if part == 1:
        bundling_info = workflow.bundling_info
        bundling_functions = workflow.bundling_functions
        min_functions = workflow.min_functions
    else:
        bundling_info = []
        bundling_functions = set()
        min_functions = set()

    total = len(func_list)
    start_functions = []
    nodes = {}
    parent_cnt = {}
    foreach_info = {}
    foreach_functions = set()
    for func in func_list:
        nodes[func] = workflow.nodes[func]
        parent_cnt[func] = workflow.parent_cnt[func]
        for pre_func in workflow.nodes[func].prev:
            if pre_func not in func_list:
                parent_cnt[func] -= 1
        if parent_cnt[func] == 0:
            start_functions.append(func)
        if func in workflow.foreach_functions:
            foreach_functions.add(func)
            foreach_info[func] = workflow.foreach_info[func]
        
    return component.workflow(workflow_name, start_functions, nodes, global_input, total, parent_cnt, 
                              foreach_functions, min_functions, bundling_functions, bundling_info, foreach_info)

# print workflow


def print_workflow(workflow: component.workflow):
    for name in workflow.nodes:
        print('function name: ', workflow.nodes[name].name)
        print('function prev: ', workflow.nodes[name].prev)
        print('function next: ', workflow.nodes[name].next)
        print('function nextDis: ', workflow.nodes[name].nextDis)
        print('function source: ', workflow.nodes[name].source)
        print('function runtime: ', workflow.nodes[name].runtime)
        print('function mem usage: ', workflow.nodes[name].mem_usage)
        print('function split_ratio: ', workflow.nodes[name].split_ratio)
        print('\n====================================')
        print('====================================\n')
    print(workflow.workflow_name)
    print(workflow.start_functions)
    print(workflow.global_input)
    print(workflow.parent_cnt)
    print(workflow.total)
    print(workflow.foreach_functions)
    print(workflow.merge_functions)

# split the workflow into three part: before shuffle, shuffle, after shuffle


def split_workflow(workflow: component.workflow):
    topo = topo_sort(workflow)
    flag = 0
    count = 0
    start = -1
    for k, v in enumerate(topo):
        if v.startswith("min"):
            if not flag:
                start = k
                flag = 1
            count += 1
    if start == -1:
        before_shuffle = topo[:]
        shuffle = []
        after_shuffle = []
    else:
        before_shuffle = topo[:start]
        shuffle = topo[start:start+count]
        after_shuffle = topo[start+count:]
    

    # before shuffle part
    w0 = create_workflow(0, before_shuffle, workflow)

    # shuffle part
    w1 = create_workflow(1, shuffle, workflow)

    # after shuffle part
    w2 = create_workflow(2, after_shuffle, workflow)

    return w0, w1, w2


# get in degree vector and init group set


def init_graph(workflow, group_set, node_info):
    global group_ip, group_scale
    ip_list = list(node_info.keys())
    in_degree_vec = dict()
    q = queue.Queue()
    for name in workflow.start_functions:
        q.put(workflow.nodes[name])
        group_set.append((name, ))
    while q.empty() is False:
        node = q.get()
        for next_node_name in node.next:
            if next_node_name not in workflow.nodes:
                continue
            if next_node_name not in in_degree_vec:
                in_degree_vec[next_node_name] = 1
                q.put(workflow.nodes[next_node_name])
                group_set.append((next_node_name, ))
            else:
                in_degree_vec[next_node_name] += 1
    for s in group_set:
        group_ip[s] = ip_list[hash(s) % len(ip_list)]
        group_scale[s] = workflow.nodes[s[0]].scale
        node_info[group_ip[s]] -= workflow.nodes[s[0]].scale
    return in_degree_vec

# Find the set in which the function resides


def find_set(node, group_set):
    for node_set in group_set:
        if node in node_set:
            return node_set
    return None


def topo_search(workflow: component.workflow, in_degree_vec, group_set):
    dist_vec = dict()  # { name: [dist, max_length] }
    prev_vec = dict()  # { name: [prev_name, length] }
    q = queue.Queue()
    for name in workflow.start_functions:
        q.put(workflow.nodes[name])
        dist_vec[name] = [workflow.nodes[name].runtime, 0]
        prev_vec[name] = []
    while q.empty() is False:
        node = q.get()
        pre_dist = dist_vec[node.name]
        prev_name = node.name
        for index in range(len(node.next)):
            next_node = workflow.nodes[node.next[index]]
            w = node.nextDis[index]
            next_node_name = next_node.name
            if next_node_name in find_set(prev_name, group_set):
                w = w / config.NET_MEM_BANDWIDTH_RATIO
            if next_node.name not in dist_vec:
                dist_vec[next_node_name] = [pre_dist[0] +
                                            w + next_node.runtime, max(pre_dist[1], w)]
                prev_vec[next_node_name] = [prev_name, w]
            elif dist_vec[next_node_name][0] < pre_dist[0] + w + next_node.runtime:
                dist_vec[next_node_name] = [pre_dist[0] +
                                            w + next_node.runtime, max(pre_dist[1], w)]
                prev_vec[next_node_name] = [prev_name, w]
            elif dist_vec[next_node_name][0] == pre_dist[0] + w + next_node.runtime and max(pre_dist[1], w) > \
                    dist_vec[next_node_name][1]:
                dist_vec[next_node_name][1] = max(pre_dist[1], w)
                prev_vec[next_node_name] = [prev_name, w]
            in_degree_vec[next_node_name] -= 1
            if in_degree_vec[next_node_name] == 0:
                q.put(next_node)
    return dist_vec, prev_vec


def mergeable(node1, node2, group_set, workflow: component.workflow, write_to_mem_nodes, node_info):
    global mem_usage, max_mem_usage, group_ip, group_scale
    node_set1 = find_set(node1, group_set)

    # same set?
    if node2 in node_set1:  # same set
        return False
    node_set2 = find_set(node2, group_set)

    # group size no larger than GROUP_LIMIT
    if len(node_set1) + len(node_set2) > config.GROUP_LIMIT:
        return False

    # meet scale requirement?
    new_node_info = node_info.copy()
    node_set1_scale = group_scale[node_set1]
    node_set2_scale = group_scale[node_set2]
    new_node_info[group_ip[node_set1]] += node_set1_scale
    new_node_info[group_ip[node_set2]] += node_set2_scale
    best_fit_addr, best_fit_scale = None, 10000000
    for addr in new_node_info:
        if new_node_info[addr] >= node_set1_scale + node_set2_scale and new_node_info[addr] < best_fit_scale:
            best_fit_addr = addr
            best_fit_scale = new_node_info[addr]
    if best_fit_addr is None:
        print('Hit scale threshold', node_set1_scale, node_set2_scale)
        return False

    # check memory limit
    if node1 not in write_to_mem_nodes:
        current_mem_usage = workflow.nodes[node1].nextDis[0] * \
            config.NETWORK_BANDWIDTH
        if mem_usage + current_mem_usage > max_mem_usage:  # too much memory consumption
            print('Hit memory consumption threshold')
            return False
        mem_usage += current_mem_usage
        write_to_mem_nodes.append(node1)

    # merge sets & update scale
    new_group_set = (*node_set1, *node_set2)

    group_set.append(new_group_set)
    group_ip[new_group_set] = best_fit_addr
    node_info[best_fit_addr] -= node_set1_scale + node_set2_scale
    group_scale[new_group_set] = node_set1_scale + node_set2_scale

    node_info[group_ip[node_set1]] += node_set1_scale
    node_info[group_ip[node_set2]] += node_set2_scale
    group_set.remove(node_set1)
    group_set.remove(node_set2)
    group_ip.pop(node_set1)
    group_ip.pop(node_set2)
    group_scale.pop(node_set1)
    group_scale.pop(node_set2)
    return True


def merge_path(crit_vec, group_set, workflow: component.workflow, write_to_mem_nodes, node_info):
    for edge in crit_vec:
        if mergeable(edge[1][0], edge[0], group_set, workflow, write_to_mem_nodes, node_info):
            return True
    return False


def get_longest_dis(workflow, dist_vec):
    dist = 0
    node_name = ''
    for name in workflow.nodes:
        if dist_vec[name][0] > dist:
            dist = dist_vec[name][0]
            node_name = name
    return dist, node_name


def grouping(workflow: component.workflow, node_info):

    # initialization: get in-degree of each node
    group_set = list()
    critical_path_functions = set()
    write_to_mem_nodes = []
    in_degree_vec = init_graph(workflow, group_set, node_info)

    while True:

        # break if every node is in same group
        if len(group_set) == 1:
            break

        # topo dp: find each node's longest dis and it's predecessor
        dist_vec, prev_vec = topo_search(
            workflow, in_degree_vec.copy(), group_set)
        crit_length, tmp_node_name = get_longest_dis(workflow, dist_vec)
        print('crit_length: ', crit_length)

        # find the longest path, edge descent sorted
        critical_path_functions.clear()
        crit_vec = dict()
        while tmp_node_name not in workflow.start_functions:
            crit_vec[tmp_node_name] = prev_vec[tmp_node_name]
            tmp_node_name = prev_vec[tmp_node_name][0]
        crit_vec = sorted(crit_vec.items(),
                          key=lambda c: c[1][1], reverse=True)
        for k, v in crit_vec:
            critical_path_functions.add(k)
            critical_path_functions.add(v[0])

        # if can't merge every edge of this path, just break
        if not merge_path(crit_vec, group_set, workflow, write_to_mem_nodes, node_info):
            break
    return group_set, critical_path_functions

# define the output destination at function level, instead of one per key/file


def get_type(workflow, node, group_detail):
    not_in_same_set = False
    in_same_set = False
    for next_node_name in node.next:
        next_node = workflow.nodes[next_node_name]
        node_set = find_set(next_node.name, group_detail)
        if node.name not in node_set:
            not_in_same_set = True
        else:
            in_same_set = True
    if not_in_same_set and in_same_set:
        return 'DB+MEM'
    elif in_same_set:
        return 'MEM'
    else:
        return 'DB'

# Get the maximum memory size available


def get_max_mem_usage(workflow: component.workflow):
    global max_mem_usage
    for name in workflow.nodes:
        if not name.startswith('virtual'):
            max_mem_usage += (1 - config.RESERVED_MEM_PERCENTAGE -
                              workflow.nodes[name].mem_usage) * config.CONTAINER_MEM * workflow.nodes[name].split_ratio
    return max_mem_usage


def save_grouping_config(workflow: component.workflow, node_info, info_dict, info_raw_dict):
    repo = repository.Repository(workflow.workflow_name)
    repo.save_function_info(
        info_dict, workflow.workflow_name + '_function_info')
    repo.save_function_info(
        info_raw_dict, workflow.workflow_name + '_function_info_raw')
    repo.save_basic_input(workflow.global_input,
                          workflow.workflow_name + '_workflow_metadata')
    repo.save_start_functions(workflow.start_functions,
                              workflow.workflow_name + '_workflow_metadata')
    repo.save_foreach_functions(
        workflow.foreach_functions, workflow.workflow_name + '_workflow_metadata')
    repo.save_min_functions(workflow.min_functions,
                              workflow.workflow_name + '_workflow_metadata')
    repo.save_bundling_functions(
        workflow.bundling_functions, workflow.workflow_name + '_workflow_metadata')
    repo.save_all_addrs(list(node_info.keys()),
                        workflow.workflow_name + '_workflow_metadata')
    repo.save_bundling_info(workflow.bundling_info,
                            workflow.workflow_name + '_workflow_metadata')
    repo.save_foreach_info(workflow.foreach_info,
                            workflow.workflow_name + '_workflow_metadata')
    # repo.save_critical_path_functions(
    #     critical_path_functions, workflow.workflow_name + '_workflow_metadata')

def find_bundling_group(name: str, group_dict: dict):
    prefix,bundling_stage_idx,bundling_group_idx = name.split('-')
    if 'bundling' in prefix:
        bundling_stage_idx = int(bundling_stage_idx)
    else:
        bundling_stage_idx = int(bundling_stage_idx) // 2
    bundling_group_idx = int(bundling_group_idx)
    try:
        group_dict['bundlingstage-{}'.format(bundling_stage_idx)][bundling_group_idx].append(name)
    except:
        if 'bundlingstage-{}'.format(bundling_stage_idx) not in group_dict:
            group_dict['bundlingstage-{}'.format(bundling_stage_idx)] = {bundling_group_idx: [name]}
        else:
            group_dict['bundlingstage-{}'.format(bundling_stage_idx)][bundling_group_idx] = [name]

def get_grouping_config(workflow: component.workflow, node_info_dict):

    global max_mem_usage, group_ip

    # grouping algorithm
    max_mem_usage = get_max_mem_usage(workflow)
    # print('max_mem_usage', max_mem_usage)
    w0, w1, w2 = split_workflow(workflow)
    node_info_list = list(node_info_dict.keys())
    node_number = len(node_info_list)
    bundling_info = workflow.bundling_info
    w0_group_detail = []
    w1_group_detail = []
    w2_group_detail = []

    if w1.nodes:
        group_dict = {}
        for node in w1.nodes:
            find_bundling_group(node, group_dict)
        for stage in group_dict.values():
            for key, group in stage.items():
                w1_group_detail.append(tuple(group))
                group_ip[tuple(group)] = node_info_list[key % node_number]

    if w0.nodes:
        w0_group_detail, w0_critical_path_functions = grouping(w0, node_info_dict)
    if w2.nodes:
        w2_group_detail, w2_critical_path_functions = grouping(w2, node_info_dict)
    group_detail = w0_group_detail + w1_group_detail + w2_group_detail
    print(group_detail)
    print(group_ip)
    print(group_scale)

    # building function info: both optmized and raw version
    ip_list = list(node_info_dict.keys())
    function_info_dict = {}
    function_info_raw_dict = {}
    for node_name in workflow.nodes:
        node = workflow.nodes[node_name]
        to = get_type(workflow, node, group_detail)
        ip = group_ip[find_set(node_name, group_detail)]
        function_info = {'function_name': node.name, 'runtime': node.runtime, 'to': to, 'ip': ip,
                         'parent_cnt': workflow.parent_cnt[node.name], 'conditions': node.conditions}
        function_info_raw = {'function_name': node.name, 'runtime': node.runtime, 'to': 'DB', 'ip': ip_list[hash(node.name) % len(ip_list)],
                             'parent_cnt': workflow.parent_cnt[node.name], 'conditions': node.conditions}
        function_input = dict()
        function_input_raw = dict()
        for arg in node.input_files:
            function_input[arg] = {'size': node.input_files[arg]['size'],
                                   'function': node.input_files[arg]['function'],
                                   'parameter': node.input_files[arg]['parameter'],
                                   'type': node.input_files[arg]['type']}
            function_input_raw[arg] = {'size': node.input_files[arg]['size'],
                                       'function': node.input_files[arg]['function'],
                                       'parameter': node.input_files[arg]['parameter'],
                                       'type': node.input_files[arg]['type']}
        function_output = dict()
        function_output_raw = dict()
        for arg in node.output_files:
            function_output[arg] = {
                'size': node.output_files[arg]['size'], 'type': node.output_files[arg]['type']}
            function_output_raw[arg] = {
                'size': node.output_files[arg]['size'], 'type': node.output_files[arg]['type']}
        function_info['input'] = function_input
        function_info['output'] = function_output
        function_info['prev'] = node.prev
        function_info['next'] = node.next
        function_info['split_ratio'] = node.split_ratio
        function_info_raw['input'] = function_input_raw
        function_info_raw['output'] = function_output_raw
        function_info_raw['prev'] = node.prev
        function_info_raw['next'] = node.next
        function_info_raw['split_ratio'] = node.split_ratio
        function_info_dict[node_name] = function_info
        function_info_raw_dict[node_name] = function_info_raw

    # if successor contains 'virtual', then the destination of storage should be propagated
    for name in workflow.nodes:
        for next_name in workflow.nodes[name].next:
            if next_name.startswith('virtual'):
                if function_info_dict[next_name]['to'] != function_info_dict[name]['to']:
                    function_info_dict[name]['to'] = 'DB+MEM'

    # , critical_path_functions
    return node_info_dict, function_info_dict, function_info_raw_dict

def test(workflow_pool):
    node_info_list = yaml.load(open('/home/k8s/little/minflow/src/grouping/node_info.yaml'), Loader=yaml.FullLoader)
    node_info_dict = {}
    for node_info in node_info_list['nodes']:
        node_info_dict[node_info['worker_address']
                       ] = node_info['scale_limit'] * 0.8

    for workflow_name in workflow_pool:
        workflow = parse_yaml_bundling.test()
        node_info, function_info, function_info_raw = get_grouping_config(
            workflow, node_info_dict)
        save_grouping_config(workflow, node_info, function_info,
                             function_info_raw)
    return list(workflow.nodes.keys())


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print('usage: python3 grouping.py <workflow_name>, ...')

    # get node info
    node_info_list = yaml.load(open('/home/k8s/little/minflow/src/grouping/node_info.yaml'), Loader=yaml.FullLoader)
    node_info_dict = {}
    for node_info in node_info_list['nodes']:
        node_info_dict[node_info['worker_address']
                       ] = node_info['scale_limit'] * 0.8

    workflow_pool = sys.argv[1:]
    split_ratio = 18
    for workflow_name in workflow_pool:
        config_path = config.FUNCTION_INFO_ADDRS
        for wfname, addr in config_path.items():
            parse_yaml_bundling.gen_workflow(addr, split_ratio)
        workflow = parse_yaml_bundling.parse(workflow_name)
        node_info, function_info, function_info_raw = get_grouping_config(
            workflow, node_info_dict)
        save_grouping_config(workflow, node_info, function_info,
                             function_info_raw)
