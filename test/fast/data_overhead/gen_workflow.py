import yaml
import os
import sys
from enum import Enum

class Mode(Enum):
    OPTIMIZED = 'optimized'
    RAW = 'raw'
    MIN = 'min'
    SINGLE = 'single'

# get all functions' infomations from configuration file
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

def change_config(data_mode: Mode, shuffle_mode: Mode):
    command = "sed -i \"s/DATA_MODE = '.*'/DATA_MODE = '{}'/g\" ../../../config/config.py".format(data_mode.value)
    os.system(command)
    command = "sed -i \"s/SHUFFLE_MODE = '.*'/SHUFFLE_MODE = '{}'/g\" ../../../config/config.py".format(shuffle_mode.value)
    os.system(command)

    pass

if __name__ == '__main__':

    split_ratio_list = [10,20,30,40,50]

    config_path = '../../../benchmark/wordcount-shuffle'

    for split_ratio in split_ratio_list:
        print('|========================================|')
        print('|========================================|')
        print('split ratio: ', split_ratio)
        gen_workflow(config_path, split_ratio)

        cur = os.getcwd()
        change_config(Mode.OPTIMIZED, Mode.MIN)
        os.chdir('../../../src/grouping/')
        os.system('python grouping_bundling.py wordcount-shuffle')
        os.chdir(cur)
        os.system('python run.py --datamode={} >> results.txt'.format(Mode.OPTIMIZED.value))

        change_config(Mode.OPTIMIZED, Mode.SINGLE)
        os.chdir('../../../src/grouping/')
        os.system('python grouping_min.py wordcount-shuffle')
        os.chdir(cur)
        os.system('python run.py --datamode={} >> results.txt'.format(Mode.OPTIMIZED.value))

        change_config(Mode.RAW, Mode.SINGLE)
        os.system('python run.py --datamode={} >> results.txt'.format(Mode.RAW.value))

    

        
   

    
            
            
            