import os
import subprocess
import pickle
import boto3
import container_config
import multiprocessing
import sys
import numpy as np
import pandas as pd
import table_schemas
import datetime
from io import StringIO, BytesIO
import pickle
import gc
import time

region_name = container_config.S3_REGION_NAME
access_key = container_config.S3_ACCESS_KEY
secret_key = container_config.S3_SECRET_KEY
remote_db = boto3.client(service_name='s3', region_name=region_name,
                                aws_access_key_id=access_key, aws_secret_access_key=secret_key,)


table_upload = ['catalog_sales', 'catalog_returns']
workflow_name = 'tpcds-16'
func_name = ['partcs', 'partcr']




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

# def gen_data(command):
#     subprocess.check_output(command)
    
#     for idx, table in enumerate(table_upload):
#         file_path = './upload/node{}/{}_{}_{}.dat'.format(node,table,command[-1],file_num)
#         bucket_path = '{}/{}/global-input-{}-{}.csv'.format(workflow_name,file_num,func_name[idx],int(command[-1])-1)
#         print(file_path)
#         remote_db.upload_file(file_path, input_bucket, bucket_path)
#         print(bucket_path)
#         pass
    
    
    
if __name__ == '__main__':
    num_processes = 20
    
    file_num_list = [int(sys.argv[1])]
    data_size_list = [int(sys.argv[2])] #328GB=100G 643GB=200G
    
    workflow_name = 'tpcds-16'
    func_name = 'partca'
    table_parent = 'customer_address'
    table_upload = 'customer_address'
    input_bucket = ['minflow-myinput','minflow-myinput2']
    for k, data_size in enumerate(data_size_list):
        subprocess.check_output(["./dsdgen", 
                                     "-dir", "./dataset", 
                                     "-table", table_parent, 
                                     "-scale", str(data_size),
                                     "-force"])

        file_path = './dataset/{}.dat'.format(table_upload)

        with open(file_path, 'rb') as f:
            data = f.read()
            table = read_table(table_upload, data)[['ca_state', 'ca_address_sk']]

        csv_buffer = BytesIO()
        table.to_csv(csv_buffer, sep="|", header=False, index=False)

        # file_path = './datedim/new{}.dat'.format(table_upload)
        with open(file_path, 'wb') as f:
            f.write(csv_buffer.getvalue())
        
        for file_num in file_num_list:
            # print(file_path)
            for i in range(file_num):
                bucket_path = f'{workflow_name}/{file_num}/global-input-{func_name}-{i}.csv'
                remote_db.upload_file(file_path, input_bucket[k], bucket_path)
                print(bucket_path)