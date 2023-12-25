import numpy as np
from io import BytesIO, StringIO
import pandas as pd
import table_schemas
import datetime
import hashlib
import pickle
import time


def get_type(typename):
    if typename == 'date':
        return datetime.datetime
    elif "decimal" in typename:
        return np.dtype("float")
    elif typename == "int" or typename == "long":
        return np.dtype("float")
    elif typename == "float":
        return np.dtype(typename)
    elif typename == "str":
        return np.dtype(typename)
    raise Exception("Not supported type: " + typename)


def get_name_for_table(tablename):
    schema = table_schemas.schemas()[tablename]
    names = [a[0] for a in schema]
    return names


def get_dtypes_for_table(tablename):
    schema = table_schemas.schemas()[tablename]
    dtypes = {}
    for a, b in schema:
        dtypes[a] = get_type(b)
    return dtypes

def hash_func(key):
    return int.from_bytes(hashlib.md5(key).digest()[-2:], byteorder='big')

def partition_csv(data: bytes, table_name, column_names, n_partitons):
    names = get_name_for_table(table_name)
    indices = [names.index(myterm) for myterm in column_names]
    res = [BytesIO() for _ in range(n_partitons)]
    print(indices)
    lines = data.splitlines()
    for line in lines:
        cols = line.split(b'|')
        key = b''.join([cols[idx] for idx in indices])
        hash_value = hash_func(key)
        res[hash_value % n_partitons].write(line + b'\n')
    return [r.getvalue() for r in res]


def main(store):
    # input
    cs_table_name = 'catalog_sales'
    cr_table_name = 'catalog_returns'
    output_keys = store.fetch_io_keys('output')
    split_ratio = store.fetch_io_keys('split_ratio')
    
    start = time.time()
    cs_data = store.fetch_global_input(func_name='partcs', suffix='.csv')
    cr_data = store.fetch_global_input(func_name='partcr', suffix='.csv')
    store.clear()
    end = time.time()
    input_time = {'start': start, 'end': end, 'latency': end-start}
    
    start = time.time()
    hash_start = 0
    hash_end = split_ratio - 1
    
    cs_parted = partition_csv(cs_data, cs_table_name, ['cs_order_number'], split_ratio)
    cr_parted = partition_csv(cr_data, cr_table_name, ['cr_order_number'], split_ratio)
    del cs_data
    del cr_data

    # split
    output_num = len(output_keys)
    step = (hash_end - hash_start + 1) // output_num
    split_output = {}
    split_output_type = {}
    for i in range(hash_start, hash_end + 1, step):
        file_name = output_keys[(i-hash_start)//step]
        serialized_data = {'content':{'catalog_sales':{}, 'catalog_returns':{}}, 'hash_info':{}}
        split_output[file_name] = {}
        split_output_type[file_name] = 'application/octet'
        for j in range(i, i + step):
            # split_output[file_name][j] = str(parted[j], encoding='utf-8')
            serialized_data['content']['catalog_sales'][str(j)] = [cs_parted[j]]
            serialized_data['content']['catalog_returns'][str(j)] = [cr_parted[j]]
            
        serialized_data['hash_info'] = {'hash_start': i,
                                            'hash_end': i + step - 1, 'split_ratio': split_ratio}
        
        split_output[file_name] = pickle.dumps(serialized_data)
        del serialized_data
        
        if store.to == 'DB+MEM':
            fname_split = file_name.split('_')
            cur_func = int(fname_split[0].split('-')[-1])
            next_func = int(fname_split[1].split('-')[-1])
            if (cur_func // store.bundling_size) % store.node == (next_func // store.bundling_size) % store.node:
                store.db_mem[file_name] = 1
            else:
                store.db_mem[file_name] = 0  

    del cs_parted
    del cr_parted
        
      
                
    end = time.time()
    compute_time = {'start': start, 'end': end, 'latency': end-start}
    
    start = time.time()
    store.put(split_output,  split_output_type)
    store.clear()
    end = time.time()
    output_time = {'start': start, 'end': end, 'latency': end-start}
    log = {'input_time': input_time, 'compute_time': compute_time, 'output_time': output_time}
    key = output_keys[0].split('_')[0]
    store.log(key, pickle.dumps(log))
    
    

