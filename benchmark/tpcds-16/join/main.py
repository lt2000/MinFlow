import numpy as np
import pandas as pd
import table_schemas
import datetime
from io import StringIO, BytesIO
import pickle
import gc
import time
import byteconcat


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

    table = pd.read_table(BytesIO(data),
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
    input_keys = store.fetch_io_keys('input')
    output_keys = store.fetch_io_keys('output')
    next_func = output_keys[0].split('_')[-1]
    cs_table_name = 'catalog_sales'
    cr_table_name = 'catalog_returns'
    if next_func != 'merge':
        start = time.time()
        input_files = store.fetch(input_keys)
        store.clear()
        end = time.time()
        input_time = {'start': start, 'end': end, 'latency': end-start}

        start = time.time()
        deserialized_data = {}
        hash_idx = {}
        for file in input_files:
            deserialized_data[file] = pickle.loads(input_files[file])
            if not hash_idx:
                hash_idx = deserialized_data[file]['hash_info']
        del input_files

        # merge sort
        cs_output_buckets = {}
        cr_output_buckets = {}
        hash_start = hash_idx['hash_start']
        hash_end = hash_idx['hash_end']
        split_ratio = hash_idx['split_ratio']

        for idx in range(hash_start, hash_end + 1):
            cs_output_buckets[idx] = []
            cr_output_buckets[idx] = []

        for fname in deserialized_data:
            file = deserialized_data[fname]['content']
            for hidx in range(hash_start, hash_end + 1):
                cs_output_buckets[hidx].extend(file[cs_table_name][str(hidx)])
                cr_output_buckets[hidx].extend(file[cr_table_name][str(hidx)])

        # split
        output_num = len(output_keys)
        step = (hash_end - hash_start + 1) // output_num
        split_output = {}
        split_output_type = {}
        for i in range(hash_start, hash_end + 1, step):
            file_name = output_keys[(i-hash_start)//step]
            serialized_data = {'content':{'catalog_sales':{}, 'catalog_returns':{}}, 'hash_info':{}}
            split_output_type[file_name] = 'application/bytes'
            for j in range(i, i + step):
                serialized_data['content'][cs_table_name][str(j)] = cs_output_buckets[j]
                serialized_data['content'][cr_table_name][str(j)] = cr_output_buckets[j]
                
                del cs_output_buckets[j]
                del cr_output_buckets[j]

            serialized_data['hash_info'] = {'hash_start': i,
                                                'hash_end': i + step - 1, 'split_ratio': split_ratio}
            split_output[file_name] = pickle.dumps(serialized_data)
            del serialized_data
            
        end = time.time()
        compute_time = {'start': start, 'end': end, 'latency': end-start}   

        start = time.time()
        store.put(split_output, split_output_type)
        store.clear()
        end = time.time()
        output_time = {'start': start, 'end': end, 'latency': end-start}
        log = {'input_time': input_time, 'compute_time': compute_time, 'output_time': output_time}
        key = output_keys[0].split('_')[0]
        store.log(key, pickle.dumps(log))    
    else:
        start = time.time()
        dd_table_name = 'date_dim'
        ca_table_name = 'customer_address'
        cc_table_name = 'call_center'
        input_files = store.fetch(input_keys)   
        end = time.time()
        input_time = {'start': start, 'end': end, 'latency': end-start}
        
        
        start = time.time()
        dd_data = store.fetch_global_input(func_name='partdd', suffix='.csv')
        ca_data = store.fetch_global_input(func_name='partca', suffix='.csv')
        cc_data = store.fetch_global_input(func_name='partcc', suffix='.csv')
        store.clear() 
        deserialized_data = {}
        hash_idx = {}
        for file in input_files:
            deserialized_data[file] = pickle.loads(input_files[file])
            if not hash_idx:
                hash_idx = deserialized_data[file]['hash_info']
        del input_files

        # merge sort
        cs_output_buckets = {}
        cr_output_buckets = {}
        hash_start = hash_idx['hash_start']
        hash_end = hash_idx['hash_end']
        split_ratio = hash_idx['split_ratio']

        for idx in range(hash_start, hash_end + 1):
            cs_output_buckets[idx] = []
            cr_output_buckets[idx] = []

        for fname in deserialized_data:
            file = deserialized_data[fname]['content']
            for hidx in range(hash_start, hash_end + 1):
                cs_output_buckets[hidx].extend(file[cs_table_name][str(hidx)])
                cr_output_buckets[hidx].extend(file[cr_table_name][str(hidx)])

        # read table
        for key in cs_output_buckets:
            wanted_columns = ['cs_order_number',
                      'cs_ext_ship_cost',
                      'cs_net_profit',
                      'cs_ship_date_sk',
                      'cs_ship_addr_sk',
                      'cs_call_center_sk',
                      'cs_warehouse_sk']
            table_cs = read_table(cs_table_name, byteconcat.concat_bytes(cs_output_buckets[key]))[wanted_columns]
            # table_cs = table_cs.sort_values(by=['cs_order_number'])
        for key in cr_output_buckets:
            table_cr = read_table(cr_table_name, byteconcat.concat_bytes(cr_output_buckets[key]))  
            # table_cr = table_cr.sort_values(by=['cr_order_number'])  
        
        del cs_output_buckets   
        del cr_output_buckets
        
        table_dd = read_table(dd_table_name, dd_data)[['d_date', 'd_date_sk']]
        table_ca = read_table(ca_table_name, ca_data)[['ca_state', 'ca_address_sk']]
        table_cc = read_table(cc_table_name, cc_data)
        del dd_data
        del ca_data
        del cc_data
        
        cs_succient = table_cs[['cs_order_number', 'cs_warehouse_sk']]
        wh_uc = cs_succient.groupby(['cs_order_number']).agg({'cs_warehouse_sk':'nunique'})
        target_order_numbers = wh_uc.loc[wh_uc['cs_warehouse_sk'] > 1].index.values
        cs_sj_f1 = table_cs.loc[table_cs['cs_order_number'].isin(target_order_numbers)]
        
        cs_sj_f2 = cs_sj_f1.loc[~cs_sj_f1['cs_order_number'].isin(table_cr.cr_order_number)]
        del cs_sj_f1

        dd_select = table_dd[(pd.to_datetime(table_dd['d_date']) > pd.to_datetime('2002-02-01')) & (pd.to_datetime(table_dd['d_date']) < pd.to_datetime('2002-04-01'))]
        dd_filtered = dd_select[['d_date_sk']]
        merged_cs = cs_sj_f2.merge(dd_filtered, left_on='cs_ship_date_sk', right_on='d_date_sk')
        del cs_sj_f2
        del dd_select
        del dd_filtered
        merged_cs.drop('d_date_sk', axis=1, inplace=True)
        ca = table_ca[table_ca.ca_state == 'GA'][['ca_address_sk']]
        del table_ca
        merged_ca = merged_cs.merge(ca, left_on='cs_ship_addr_sk', right_on='ca_address_sk')
        merged_ca.drop('cs_ship_addr_sk', axis=1, inplace=True)
  

        list_addr = ['Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County']
        cc_p = table_cc[table_cc.cc_county.isin(list_addr)][['cc_call_center_sk']]

        merged_cc = merged_ca.merge(cc_p, left_on='cs_call_center_sk', right_on='cc_call_center_sk')[['cs_order_number', 'cs_ext_ship_cost', 'cs_net_profit']]

        if merged_cc.empty:
            end = time.time()
            compute_time = {'start': start, 'end': end, 'latency': end-start}
            start = time.time()
            end = time.time()
            output_time = {'start': start, 'end': end, 'latency': end-start}
            log = {'input_time': input_time, 'compute_time': compute_time, 'output_time': output_time}
            key = output_keys[0].split('_')[0]
            store.log(key, pickle.dumps(log)) 
            return
        csv_buffer = StringIO()
        merged_cc.to_csv(csv_buffer, sep="|", header=False, index=False)
        merged_cc = csv_buffer.getvalue()
        res = {}
        res_type = {}
        for name in output_keys:
            res[name] = pickle.dumps(merged_cc) 
            res_type[name] = 'text/csv'
        end = time.time()
        compute_time = {'start': start, 'end': end, 'latency': end-start}   
    
        start = time.time()
        store.put(res, res_type)
        store.clear()
        end = time.time()
        output_time = {'start': start, 'end': end, 'latency': end-start}
        log = {'input_time': input_time, 'compute_time': compute_time, 'output_time': output_time}
        key = output_keys[0].split('_')[0]
        store.log(key, pickle.dumps(log)) 
        
    
        
    
    
    

