import yaml
import os
import sys
import component
from typing import Dict
sys.path.append('../../config')
import config

# get all functions' infomations from configuration file
def gen_function_info(config_path: str, nodes: Dict[str, component.function]):
    function_info = []
    config_file = os.path.join(config_path, "function_info.yaml")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        workflow_name = config['workflow']
        max_containers = config['max_containers']
        functions = {}
        for c in config['functions']:
            functions[c['name']] = {'image': c['image'], 'name': c['name'], 'qos_time': c['qos_time'], 'qos_requirement': c['qos_requirement']}

    config_file = os.path.join(config_path, "new_function_info.yaml")
    with open(config_file, 'w') as f:
        yaml_file = {}
        yaml_file['workflow'] = workflow_name
        yaml_file['max_containers'] = max_containers

        yaml_file['functions'] = []

        for func in nodes:
            if func in functions:
                yaml_file['functions'].append(functions[func])
            else:
                for fn in functions:
                    if fn in func:
                        yaml_file['functions'].append({'image': nodes[func].source, 'name': func, 'qos_time': functions[fn]['qos_time'], 'qos_requirement': functions[fn]['qos_requirement']})
        yaml.dump(yaml_file,f)


 


if __name__ == '__main__':

    config_path = config.FUNCTION_INFO_ADDRS
    for wfname, addr in config_path.items():
        pass
    nodes = {}
    parse(addr, nodes)

    
            
            
            