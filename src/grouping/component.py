from typing import Dict


class function:
    def __init__(self, name, prev, next, nextDis, source, runtime, input_files, output_files, conditions):
        self.name = name
        self.prev = prev
        self.next = next
        self.nextDis = nextDis
        self.source = source
        self.runtime = runtime
        self.input_files = input_files
        self.output_files = output_files
        self.conditions = conditions
        self.scale = 0
        self.mem_usage = 0
        self.split_ratio = 1

    def set_scale(self, scale):
        self.scale = scale

    def set_mem_usage(self, mem_usage):
        self.mem_usage = mem_usage

    # foreach: multiple container in one workflow
    def set_split_ratio(self, split_ratio):
        self.split_ratio = split_ratio


class workflow:
    def __init__(self, workflow_name, start_functions, nodes: Dict[str, function], global_input, total, parent_cnt, 
                 foreach_functions, min_functions, bundling_functions, bundling_info, foreach_info):
        self.workflow_name = workflow_name
        self.start_functions = start_functions
        self.nodes = nodes  # dict: {name: function()}
        self.global_input = global_input
        self.total = total
        self.parent_cnt = parent_cnt  # dict: {name: parent_cnt}
        self.foreach_functions = foreach_functions
        self.min_functions = min_functions
        self.bundling_functions = bundling_functions
        self.bundling_info = bundling_info
        self.foreach_info = foreach_info
