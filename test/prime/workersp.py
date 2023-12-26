import sys
import gevent
import gevent.lock
import logging
import time

import repository
from typing import Any, Dict, List
import requests
from enum import Enum

sys.path.append('../function_manager')
from function_manager import FunctionManager
sys.path.append('../../config')
import config
repo = repository.Repository()
shuffle_mode = config.SHUFFLE_MODE
node_num = config.NODE_NUM


class FuncType(Enum):
    NORMAL = 0
    FOREACH = 1
    BUNDLING = 2
    MIN = 3


class FakeFunc:
    def __init__(self, req_id: str, func_name: str):
        self.req_id = req_id
        self.func = func_name

    def __getattr__(self, name: str):
        return repo.fetch(self.req_id, name)


def cond_exec(req_id: str, cond: str) -> Any:
    if cond.startswith('default'):
        return True

    values = {}
    res = None
    while True:
        try:
            res = eval(cond, values)
            break
        except NameError as e:
            name = str(e).split("'")[1]
            values[name] = FakeFunc(req_id, name)
    return res


class WorkflowState:
    def __init__(self, request_id: str, all_func: List[str]):
        self.request_id = request_id
        self.lock = gevent.lock.BoundedSemaphore()  # guard the whole state

        self.executed: Dict[str, bool] = {}
        self.parent_executed: Dict[str, int] = {}
        for f in all_func:
            self.executed[f] = False
            self.parent_executed[f] = 0


min_port = 20000

# mode: 'optimized' vs 'normal'


class WorkerSPManager:
    def __init__(self, host_addr: str, workflow_name: str, data_mode: str, function_info_addr: str):
        global min_port

        self.lock = gevent.lock.BoundedSemaphore()  # guard self.states
        self.host_addr = host_addr
        self.workflow_name = workflow_name
        self.states: Dict[str, WorkflowState] = {}
        self.function_info: Dict[str, dict] = {}

        self.data_mode = data_mode
        if data_mode == 'optimized':
            self.info_db = workflow_name + '_function_info'
        else:
            self.info_db = workflow_name + '_function_info_raw'
        self.meta_db = workflow_name + '_workflow_metadata'

        self.foreach_func = repo.get_foreach_functions(self.meta_db)
        self.bundling_func = repo.get_bundling_functions(self.meta_db)
        self.min_func = repo.get_min_functions(self.meta_db)
        self.bundling_info = repo.get_bundling_info(self.meta_db)
        self.foreach_info = repo.get_foreach_info(self.meta_db)
        self.func = repo.get_current_node_functions(
            self.host_addr, self.info_db)

        self.function_manager = FunctionManager(function_info_addr, min_port)
        self.ft = FuncType
        min_port += 5000

    # return the workflow state of the request
    def get_state(self, request_id: str) -> WorkflowState:
        self.lock.acquire()
        if request_id not in self.states:
            self.states[request_id] = WorkflowState(request_id, self.func)
        state = self.states[request_id]
        self.lock.release()
        return state

    def del_state_remote(self, request_id: str, remote_addr: str):
        url = 'http://{}/clear'.format(remote_addr)
        requests.post(url, json={'request_id': request_id,
                      'workflow_name': self.workflow_name})

    # delete state
    def del_state(self, request_id: str, master: bool):
        logging.info('delete state of: %s', request_id)
        self.lock.acquire()
        if request_id in self.states:
            del self.states[request_id]
        self.lock.release()
        if master:
            jobs = []
            addrs = repo.get_all_addrs(self.meta_db)
            for addr in addrs:
                if addr != self.host_addr:
                    jobs.append(gevent.spawn(
                        self.del_state_remote, request_id, addr))
            gevent.joinall(jobs)

    # get function's info from database
    # the result is cached
    def get_function_info(self, function_name: str) -> Any:
        if function_name not in self.function_info:
            # print(function_name)
            self.function_info[function_name] = repo.get_function_info(
                function_name, self.info_db)
        return self.function_info[function_name]

    # trigger the function when one of its parent is finished
    # function may run or not, depending on if all its parents were finished
    # function could be local or remote
    def trigger_function(self, state: WorkflowState, function_name: str, no_parent_execution=False) -> None:
        func_info = self.get_function_info(function_name)
        if func_info['ip'] == self.host_addr:
            # function runs on local

            self.trigger_function_local(
                state, function_name, no_parent_execution)
        else:
            # function runs on remote machine
            self.trigger_function_remote(
                state, function_name, func_info['ip'], no_parent_execution)

    # trigger a function that runs on local
    def trigger_function_local(self, state: WorkflowState, function_name: str, no_parent_execution=False) -> None:
        logging.info('trigger local function: %s of: %s',
                     function_name, state.request_id)
        state.lock.acquire()
        if not no_parent_execution:
            state.parent_executed[function_name] += 1
        runnable = self.check_runnable(state, function_name)
        # remember to release state.lock
        if runnable:
            state.executed[function_name] = True
            state.lock.release()
            self.run_function(state, function_name)
        else:
            state.lock.release()

    # trigger a function that runs on remote machine
    def trigger_function_remote(self, state: WorkflowState, function_name: str, remote_addr: str, no_parent_execution=False) -> None:
        logging.info('trigger remote function: %s on: %s of: %s',
                     function_name, remote_addr, state.request_id)
        remote_url = 'http://{}/request'.format(remote_addr)
        data = {
            'request_id': state.request_id,
            'workflow_name': self.workflow_name,
            'function_name': function_name,
            'no_parent_execution': no_parent_execution,
        }
        response = requests.post(remote_url, json=data)
        response.close()

    # check if a function's parents are all finished
    def check_runnable(self, state: WorkflowState, function_name: str) -> bool:
        info = self.get_function_info(function_name)
        return state.parent_executed[function_name] == info['parent_cnt'] and not state.executed[function_name]

    # run a function on local
    def run_function(self, state: WorkflowState, function_name: str) -> None:
        logging.info('run function: %s of: %s',
                     function_name, state.request_id)
        # end functions
        if function_name == 'END':
            return

        info = self.get_function_info(function_name)
        # switch functions
        if function_name.startswith('virtual'):
            self.run_switch(state, info)
            return  # do not need to check next

        if function_name in self.bundling_func:  # bundling functions
            self.run_bundling(state, info)
        elif function_name in self.min_func:     # min functions
            self.run_min(state, info)
        elif function_name in self.foreach_func:  # foreach functions
            self.run_foreach(state, info)
        else:  # normal functions
            self.run_normal(state, info)

        # trigger next functions
        jobs = [
            gevent.spawn(self.trigger_function, state, func)
            for func in info['next']
        ]
        gevent.joinall(jobs)

    def baseline_hash(self, idx, function_stage_idx):
        input = []
        output = []
        split_ratio = self.bundling_info['split_ratio']
        group_size = self.bundling_info['group_size']
        function_stage_num = len(group_size)
        # output
        if function_stage_idx != function_stage_num - 1:
            for next_function_idx in range(split_ratio):
                if (idx // group_size[function_stage_idx+1]) == (next_function_idx // group_size[function_stage_idx+1]) and idx % group_size[function_stage_idx] == (next_function_idx // (group_size[function_stage_idx+1] // group_size[function_stage_idx])) % group_size[function_stage_idx]:
                    output.append(next_function_idx)
        # input
        if function_stage_idx != 0:
            for pre_function_idx in range(split_ratio):
                if (pre_function_idx // group_size[function_stage_idx]) == (idx // group_size[function_stage_idx]) and pre_function_idx % group_size[function_stage_idx-1] == (idx // (group_size[function_stage_idx] // group_size[function_stage_idx-1])) % group_size[function_stage_idx-1]:
                    input.append(pre_function_idx)

        return input, output

    def schedule_hash(self, idx, function_stage_idx, input, output):
        new_input = []
        new_output = []
        group_size = self.bundling_info['group_size']
        group_ratio = [group_size[i] // group_size[i-1] for i in range(1, len(group_size))]
        hash_input = []
        hash_output = []
        hash_ioput = []
        fucntion_stage_num = len(group_size)
        for i in range(fucntion_stage_num):
            if i % 2 == 0 and i != 0 and i !=fucntion_stage_num - 1:
                hash_ioput.append(i)
                hash_output.append(i-1)
                hash_input.append(i+1)

        # print(hash_ioput, hash_input, hash_output)
        if function_stage_idx in hash_ioput:
            origin_idx = (idx % group_ratio[function_stage_idx])*group_size[function_stage_idx] + (
                idx//group_ratio[function_stage_idx])%group_size[function_stage_idx] + (
                idx//group_size[function_stage_idx+1])*group_size[function_stage_idx+1]
            new_input, new_output = self.baseline_hash(origin_idx, function_stage_idx)

        elif function_stage_idx in hash_input and function_stage_idx in hash_output:
            for base_idx in input:
                 new_input.append((base_idx // group_size[function_stage_idx - 1])%group_ratio[function_stage_idx - 1] + (
                     base_idx % group_size[function_stage_idx - 1]) * group_ratio[function_stage_idx - 1] + (
                     base_idx//group_size[function_stage_idx])*group_size[function_stage_idx])
            for base_idx in output:
                new_output.append((base_idx // group_size[function_stage_idx+1])%group_ratio[function_stage_idx+1] + (
                     base_idx % group_size[function_stage_idx+1]) * group_ratio[function_stage_idx+1] + (
                     base_idx//group_size[function_stage_idx+2])*group_size[function_stage_idx+2])


        elif function_stage_idx in hash_input:
            new_output = output
            for base_idx in input:
                 new_input.append((base_idx // group_size[function_stage_idx - 1])%group_ratio[function_stage_idx - 1] + (
                     base_idx % group_size[function_stage_idx - 1]) * group_ratio[function_stage_idx - 1] + (
                     base_idx//group_size[function_stage_idx])*group_size[function_stage_idx])

        elif function_stage_idx in hash_output:
            new_input = input
            for base_idx in output:
                new_output.append((base_idx // group_size[function_stage_idx+1])%group_ratio[function_stage_idx+1] + (
                     base_idx % group_size[function_stage_idx+1]) * group_ratio[function_stage_idx+1] + (
                     base_idx//group_size[function_stage_idx+2])*group_size[function_stage_idx+2])
        else:
            new_input = input
            new_output = output

        return new_input, new_output

    def schedule(self, split_ratio, bundling_stage_idx, bundling_group_idx, bundling_intragroup_idx, phase):
        input = []
        output = []
        function_stage_idx = 2*bundling_stage_idx + phase
        idx = bundling_group_idx * \
            split_ratio + bundling_intragroup_idx
        # baseline network
        input, output = self.baseline_hash(idx, function_stage_idx)

        # schedule network
        input, output = self.schedule_hash(
            idx, function_stage_idx, input, output)

        return input, output

    def paser_funcname(self, name: str):
        split_name = name.split('-')

        if len(split_name) == 2:  # reduce-i
            return int(split_name[1])
        elif len(split_name) == 3:
            return int(split_name[1]), int(split_name[2])  # bundling-i-j
        elif len(split_name) == 4:
            # bundling-i-j-k
            return int(split_name[1]), int(split_name[2]), int(split_name[3])

    def gen_io_keys(self, func_name, info, func_type):
        input_keys = []
        output_keys = []
        if func_type == self.ft.NORMAL:  # func is normal
            input_keys = []
            output_keys = []
            # input keys
            for prev_func in info['prev']:
                if prev_func in self.bundling_func:
                    bundling_stage_idx, bundling_group_idx = self.paser_funcname(
                        prev_func)
                    function_stage_idx = 2*bundling_stage_idx + 1
                    bundling_size = self.foreach_info[prev_func]
                    for idx in range(bundling_size):
                        input_keys.append('min-{}-{}'.format(function_stage_idx,
                                          bundling_group_idx*bundling_size + idx) + '_' + func_name)
                elif prev_func in self.min_func:
                    func_stage_idx, func_group_idx = self.paser_funcname(
                    prev_func)
                    group_size = self.foreach_info[prev_func]
                    for idx in range(group_size):
                        input_keys.append(
                            'min-{}-{}'.format(func_stage_idx, func_group_idx*group_size + idx) + '_' + func_name)
                elif prev_func in self.foreach_func:
                    for idx in range(self.foreach_info[prev_func]):
                        input_keys.append(prev_func + '-' +
                                          str(idx) + '_' + func_name)
                else:
                    input_keys.append(prev_func + '_' + func_name)

            # output keys
            for next_func in info['next']:
                if next_func in self.bundling_func:
                    pass
                elif next_func in self.min_func:
                    func_stage_idx, func_group_idx = self.paser_funcname(
                        next_func)
                    group_size = self.foreach_info[next_func]
                    for idx in range(group_size):
                        output_keys.append(
                            func_name + '_' + 'min-{}-{}'.format(func_stage_idx, func_group_idx*group_size + idx))
                elif next_func in self.foreach_func:
                    for idx in range(self.foreach_info[next_func]):
                        output_keys.append(
                            func_name + '_' + next_func + '-' + str(idx))
                else:
                    output_keys.append(func_name + '_' + next_func)

            if not info['next']:  # current function is the last function
                output_keys.append(func_name + '_' + 'global-output')

            keys = {1: {'input': input_keys, 'output': output_keys, 'bundling_size': 1}}

        elif func_type == self.ft.FOREACH:  # func is foreach
            input_keys = {}
            output_keys = {}
            # input keys
            for i in range(info['split_ratio']):
                input_keys[i] = []

            for prev_func in info['prev']:
                if prev_func in self.bundling_func:
                    bundling_stage_idx, bundling_group_idx = self.paser_funcname(
                        prev_func)
                    function_stage_idx = 2*bundling_stage_idx+1
                    bundling_size = self.foreach_info[prev_func]
                    for idx in range(bundling_size):
                        input_keys[idx + bundling_size * bundling_group_idx].append('min-{}-{}'.format(
                            function_stage_idx, bundling_group_idx*bundling_size + idx) + '_' + func_name + '-' + str(
                            idx + bundling_size * bundling_group_idx))
                elif prev_func in self.min_func:
                    func_stage_idx, func_group_idx = self.paser_funcname(
                        prev_func)
                    group_size = self.foreach_info[prev_func]
                    for idx in range(group_size):
                        input_keys[idx].append('min-{}-{}'.format(
                            func_stage_idx, func_group_idx*group_size + idx) + '_' + func_name + '-' + str(
                            idx))
                elif prev_func in self.foreach_func:  # one to one
                    for idx in range(self.foreach_info[prev_func]):
                        input_keys[idx].append(
                            prev_func + '-' + str(idx) + '_' + func_name + '-' + str(idx))
                else:
                    for idx in range(info['split_ratio']):
                        input_keys[idx] = [
                            prev_func + '_' + func_name + '-' + str(idx)]

            # output keys
            for i in range(info['split_ratio']):
                output_keys[i] = []

            for next_func in info['next']:
                if next_func in self.bundling_func:
                    pass
                elif next_func in self.min_func:
                    func_stage_idx, func_group_idx = self.paser_funcname(
                        next_func)
                    group_size = self.foreach_info[next_func]
                    for idx in range(group_size):
                        input_keys[func_group_idx*group_size + idx].append(func_name + '-' + str(
                            idx + group_size * func_group_idx) + '_' + 'min-{}-{}'.format(
                            func_stage_idx, func_group_idx*group_size + idx))
                elif next_func in self.foreach_func:
                    for idx in range(self.foreach_info[next_func]):
                        input_keys[idx].append(
                            func_name + '-' + str(idx) + '_' + prev_func + '-' + str(idx))
                else:
                    for idx in range(info['split_ratio']):
                        output_keys[idx].append(
                            func_name + '-' + str(idx) + '_' + next_func)

            if not info['next']:  # current fucntion is the last function
                for idx in range(info['split_ratio']):
                    output_keys[idx].append(
                        func_name + '-' + str(idx) + '_' + 'global-output')

            return input_keys, output_keys

        elif func_type == self.ft.MIN:  # func is min func
            input_keys = {}
            output_keys = {}
            func_stage_idx, func_group_idx = self.paser_funcname(
                        func_name)
            # input keys
            for i in range(info['split_ratio']):
                input_keys[i] = []
            flag = 1
            for prev_func in info['prev']:
                if prev_func in self.bundling_func or prev_func in self.min_func: # current func is reduce or other min func
                    if flag:
                        for i in range(info['split_ratio']):
                            if shuffle_mode == 'min':
                                func_idx = i + func_group_idx * info['split_ratio']
                                input = []
                                output = []
                                # baseline network
                                input, output = self.baseline_hash(func_idx, func_stage_idx)
                                # schedule network
                                input, _ = self.schedule_hash(func_idx, func_stage_idx, input, output)
                                # print(idx, input)
                                for key in input:
                                    input_keys[i].append('min-{}-{}'.format(
                                        func_stage_idx - 1, key) + '_' + 'min-{}-{}'.format(
                                        func_stage_idx, func_idx))
                            elif shuffle_mode == 'single':
                                foreach_size = 10
                                func_idx = i + func_group_idx * max(info['split_ratio'],foreach_size)
                                
                                for key in range(self.bundling_info['split_ratio']):
                                    input_keys[i].append('min-{}-{}'.format(
                                        func_stage_idx - 1, key) + '_' + 'min-{}-{}'.format(
                                        func_stage_idx, func_idx))
                    flag = 0
                elif prev_func in self.foreach_func:  # one to one
                    for i in range(info['split_ratio']):
                        func_idx = i + func_group_idx * info['split_ratio']
                        input_keys[i].append(
                            prev_func + '-' + str(func_idx) + '_' + 'min-{}-{}'.format(
                                func_stage_idx, func_idx))
                else:
                    for i in range(info['split_ratio']):
                        func_idx = i + func_group_idx * info['split_ratio']
                        input_keys[i].append(prev_func + '_' + 'min-{}-{}'.format(
                                func_stage_idx, func_idx))

            
            # output keys
            for i in range(info['split_ratio']):
                output_keys[i] = []

            for next_func in info['next']:
                if next_func in self.bundling_func or next_func in self.min_func:
                    for i in range(info['split_ratio']):
                        if shuffle_mode == 'min':
                            func_idx = i + func_group_idx * info['split_ratio']
                            input = []
                            output = []
                            # baseline network
                            input, output = self.baseline_hash(func_idx, func_stage_idx)
                            # schedule network
                            _, output = self.schedule_hash(func_idx, func_stage_idx, input, output)
                            # print(idx, input)
                            for key in output:
                                output_keys[i].append('min-{}-{}'.format(
                                    func_stage_idx, func_idx) + '_' + 'min-{}-{}'.format(
                                    func_stage_idx + 1, key))
                        elif shuffle_mode == 'single':
                            foreach_size = 10
                            func_idx = i + func_group_idx * max(info['split_ratio'],foreach_size)
                            for key in range(self.bundling_info['split_ratio']):
                                output_keys[i].append('min-{}-{}'.format(
                                    func_stage_idx, func_idx) + '_' + 'min-{}-{}'.format(
                                    func_stage_idx + 1, key))
                    break
                elif next_func in self.foreach_func:
                    for i in range(info['split_ratio']):
                        func_idx = i + func_group_idx * info['split_ratio']
                        output_keys[i].append(
                            'min-{}-{}'.format(
                                func_stage_idx, func_idx) + '_' + next_func + '-' + str(i))
                else:
                    for i in range(info['split_ratio']):
                        func_idx = i + func_group_idx * info['split_ratio']
                        output_keys[i].append(
                            'min-{}-{}'.format(
                                func_stage_idx, func_idx) + '_' + next_func)

            if not info['next']:  # current fucntion is the last function
                for i in range(info['split_ratio']):
                    if shuffle_mode == 'min':
                        func_idx = i + func_group_idx * info['split_ratio']
                    else:
                        foreach_size = 10
                        func_idx = i + func_group_idx * max(info['split_ratio'],foreach_size)
                    output_keys[i].append(
                        'min-{}-{}'.format(
                            func_stage_idx, func_idx) + '_' + 'global-output')
            
            return input_keys, output_keys

        elif func_type == self.ft.BUNDLING:  # func is bundling
            input_keys = []
            output_keys = []
            bundling_stage_idx, bundling_group_idx, bundling_intragroup_idx = self.paser_funcname(
                func_name)
            bundling_size = self.bundling_info['split_ratio'] // self.bundling_info['bundling_foreach_num'][bundling_stage_idx]
            new_input = []
            new_output = []
            idx = bundling_group_idx*bundling_size + bundling_intragroup_idx

            # first phase in a bundled function
            function_stage_idx = 2*bundling_stage_idx
            for prev_func in info['prev']:
                if prev_func in self.bundling_func or prev_func in self.min_func:
                    input_keys, output_keys = self.schedule(
                    bundling_size, bundling_stage_idx, bundling_group_idx, bundling_intragroup_idx, 0)
                
                    # name intermediate file
                    for f in input_keys:
                        new_input.append(
                            'min-{}-{}_min-{}-{}'.format(function_stage_idx-1, f, function_stage_idx, idx))
                    for f in output_keys:
                        new_output.append(
                            'min-{}-{}_min-{}-{}'.format(function_stage_idx, idx, function_stage_idx + 1, f))

                    break

            
            first_phase = {'input': new_input, 'output': new_output, 'split_ratio': self.bundling_info['split_ratio']}

            
            # second phase in a bundled function
            input_keys = []
            output_keys = []
            new_input = []
            new_output = []
            function_stage_idx = 2*bundling_stage_idx + 1

            input_keys, output_keys = self.schedule(
                    bundling_size, bundling_stage_idx, bundling_group_idx, bundling_intragroup_idx, 1)
            for f in input_keys:
                        new_input.append(
                            'min-{}-{}_min-{}-{}'.format(function_stage_idx-1, f, function_stage_idx, idx))
                        
            for next_func in info['next']:
                if next_func in self.bundling_func or next_func in self.min_func:
                    # name intermediate file
                    for f in output_keys:
                        new_output.append(
                            'min-{}-{}_min-{}-{}'.format(function_stage_idx, idx, function_stage_idx + 1, f))
                    break

                elif next_func in self.foreach_func:
                    # name intermediate file
                    new_output.append(
                        'min-{}-{}'.format(function_stage_idx, idx) + '_' + next_func + str(idx))
                else:
                    # name intermediate file
                    new_output.append(
                        'min-{}-{}'.format(function_stage_idx, idx) + '_' + next_func)

           
                
            if not new_output:  # current function is the last function
                new_output.append(
                    'min-{}-{}_global-output'.format(function_stage_idx, idx))

            second_phase = {'input': new_input, 'output': new_output, 'split_ratio': self.bundling_info['split_ratio']}
            keys = {0: first_phase, 1: second_phase}

        return keys

    def run_switch(self, state: WorkflowState, info: Any) -> None:
        for i, next_func in enumerate(info['next']):
            cond = info['conditions'][i]
            if cond_exec(state.request_id, cond):
                self.trigger_function(state, next_func)
                break

    def run_bundling(self, state: WorkflowState, info: Any) -> None:
        start = time.time()

        jobs = []

        bundling_size = info['split_ratio']
        for i in range(bundling_size):
            func_name = info['function_name'] + '-' + str(i)
            keys = self.gen_io_keys(func_name, info, self.ft.BUNDLING)
            # print(keys)
            jobs.append(gevent.spawn(self.function_manager.run, info['function_name'], state.request_id,
                                     info['runtime'], info['input'], info['output'],
                                     info['to'], keys))

        gevent.joinall(jobs)
        end = time.time()
        # repo.save_latency({'request_id': state.request_id,
        #                   'function_name': info['function_name'], 'phase': 'all', 'time': end - start})

    def run_foreach(self, state: WorkflowState, info: Any) -> None:
        start = time.time()

        jobs = []
        input_keys, output_keys = self.gen_io_keys(
            info['function_name'], info, self.ft.FOREACH)
        # print('input keys', input_keys)
        # print('output keys', output_keys)
        for i in range(info['split_ratio']):
            keys = {1: {'input': input_keys[i], 'output': output_keys[i]}}
            jobs.append(gevent.spawn(self.function_manager.run, info['function_name'], state.request_id,
                                     info['runtime'], info['input'], info['output'],
                                     info['to'], keys))
        gevent.joinall(jobs)
        end = time.time()
        # repo.save_latency({'request_id': state.request_id,
        #                   'function_name': info['function_name'], 'phase': 'all', 'time': end - start})

    def run_min(self, state: WorkflowState, info: Any) -> None:
        start = time.time()

        jobs = []
        input_keys, output_keys = self.gen_io_keys( 
            info['function_name'], info, self.ft.MIN)
        # print('input keys', input_keys)
        # print('output keys', output_keys)
        # print('to', info['to'])
                 

        for i in range(info['split_ratio']):
            keys = {1: {'input': input_keys[i], 'output': output_keys[i], 
                        'split_ratio': self.bundling_info['split_ratio'], 'bundling_size': info['split_ratio']}}
            # print(keys)
            jobs.append(gevent.spawn(self.function_manager.run, info['function_name'], state.request_id,
                                     info['runtime'], info['input'], info['output'],
                                     info['to'], keys))
        gevent.joinall(jobs)
        end = time.time()
        # repo.save_latency({'request_id': state.request_id,
        #                   'function_name': info['function_name'], 'phase': 'all', 'time': end - start})

    def run_normal(self, state: WorkflowState, info: Any) -> None:
        start = time.time()
        keys = self.gen_io_keys(info['function_name'], info, self.ft.NORMAL)
        # print(keys)
        self.function_manager.run(info['function_name'], state.request_id,
                                  info['runtime'], info['input'], info['output'],
                                  info['to'], keys)
        end = time.time()
        # repo.save_latency({'request_id': state.request_id,
        #                   'function_name': info['function_name'], 'phase': 'all', 'time': end - start})

    def clear_mem(self, request_id):
        repo.clear_mem(request_id)

    def clear_db(self, request_id):
        repo.clear_db(request_id)
