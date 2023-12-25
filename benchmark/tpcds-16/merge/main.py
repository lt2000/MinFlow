import numpy as np
import pandas as pd
import table_schemas
import datetime
from io import StringIO, BytesIO
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


def read_table(table_name, data):
    names = get_name_for_table(table_name)
    dtypes = get_dtypes_for_table(table_name)
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("str")

    table = pd.read_table(StringIO(data),
            delimiter="|",
            header=None,
            names=names,
            usecols=range(len(names)),
            dtype=dtypes,
            na_values="-",
            parse_dates=parse_dates)
    return table


def main(store):
    # input
    start = time.time()
    input_keys = store.fetch_io_keys('input')
    print(input_keys, flush=True)
    output_keys = store.fetch_io_keys('output')
    input_files = store.fetch(input_keys)
    store.clear()
    end = time.time()
    input_time = {'start': start, 'end': end, 'latency': end-start}
    start = time.time()
    merge_data = ''
    for file in input_files:
        if input_files[file] != 'NoSuchKey':
            merge_data += pickle.loads(input_files[file])
    del input_files
    merge_table_name = 'catalog_sales'
    table_m = read_table(merge_table_name, merge_data)
    a1 = pd.unique(table_m['cs_order_number']).size
    a2 = table_m['cs_ext_ship_cost'].sum()
    a3 = table_m['cs_net_profit'].sum()
    end = time.time()
    compute_time = {'start': start, 'end': end, 'latency': end-start}  
    
     
    start = time.time()
    res = {}
    res_type = {}
    for name in output_keys:
        res[name] = pickle.dumps({'order count':a1, 'total shipping cost': a2, 'total net profit': a3}) 
        res_type[name] = 'text/csv'
    store.put(res, res_type)
    store.clear()
    end = time.time()
    output_time = {'start': start, 'end': end, 'latency': end-start}
    log = {'input_time': input_time, 'compute_time': compute_time, 'output_time': output_time}
    key = output_keys[0].split('_')[0]
    store.log(key, pickle.dumps(log))    
    
    
    
    

