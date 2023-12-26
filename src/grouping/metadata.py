import os 
import sys
import yaml



if __name__ == '__main__':
    ip_list = []
    node_info_list = yaml.load(open('node_info.yaml'), Loader=yaml.FullLoader)
    for node_info in node_info_list['nodes']:
        ip_list.append(node_info['worker_address'].split(':')[0])
        
    split_ratio = int(sys.argv[1])
    workflow_name = sys.argv[2]
    for k,ip in enumerate(ip_list):
        os.system(f'python grouping_min.py {split_ratio} {ip} {workflow_name} ')
        print(f'node{k+1} success!')