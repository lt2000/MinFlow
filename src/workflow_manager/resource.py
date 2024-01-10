import time
import psutil
import pandas as pd
import subprocess
import sys
import os
import numpy as np


# node_dict ={'172.31.34.109': '1',
#             '172.31.46.163': '2',
#             '172.31.33.210': '3',
#             '172.31.44.9'  : '4',
#             '172.31.34.237': '5',
#             '172.31.47.234': '6',
#             '172.31.41.149': '7',
#             '172.31.45.246': '8',
#             '172.31.33.191':'9',
#             '172.31.36.98' :'10'
# }

# Follow the format 'node_ip':'node_id' to configure
node_dict ={'172.31.42.166': '1',
            '172.31.35.99': '2'
}

def write_to_file(data,path):
    with open(path, 'a') as file:
        file.write(data)
        file.write('\n')

def read_resource_usage(filename='resource_usage_node6.txt'):
    names = ['sent', 'recv', 'cpu', 'mem']
    dtypes = {'sent': 'float64', 'recv': 'float64', 'cpu': 'float64', 'mem': 'float64'}
    df = pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes)
    print(df)
    print(df['sent'])


def get_resource_statistics(interval=1, node=0, req_id=''):
    result = subprocess.check_output(f"ip route | grep default | awk \'{{print $5}}\'", shell=True)
    interface = result.strip().decode()
    # print(interface)
    last_time = time.time()
    last_bytes_sent = psutil.net_io_counters(pernic=True)[interface].bytes_sent
    last_bytes_received = psutil.net_io_counters(
        pernic=True)[interface].bytes_recv
    
    # os.system('mkdir ~/dataset/')
    path = '/home/ubuntu/dataset/{}_node{}.txt'.format(req_id, node_dict[node])
    # if os.path.exists(path):
    #     os.remove(path)
    time.sleep(interval) 
    while True:
        current_time = time.time()
        current_bytes_sent = psutil.net_io_counters(
            pernic=True)[interface].bytes_sent
        current_bytes_received = psutil.net_io_counters(pernic=True)[
            interface].bytes_recv

        elapsed_time = current_time - last_time
        sent_throughput = (current_bytes_sent - last_bytes_sent) / elapsed_time
        received_throughput = (current_bytes_received -
                               last_bytes_received) / elapsed_time

        last_time = current_time
        last_bytes_sent = current_bytes_sent
        last_bytes_received = current_bytes_received

        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        
        time.sleep(interval)
        data = f"{sent_throughput:.2f}|{received_throughput:.2f}|{cpu_usage:.2f}|{memory_usage:.2f}"
        write_to_file(data,path)

        

if __name__ == "__main__":
    node = sys.argv[1]
    req_id = sys.argv[2]
    get_resource_statistics(interval=0.05, node=node, req_id=req_id)
    # read_resource_usage()