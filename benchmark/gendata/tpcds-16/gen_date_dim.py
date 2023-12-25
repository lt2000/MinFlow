import os
import subprocess
import pickle
import boto3
import container_config
import multiprocessing
import sys

region_name = container_config.S3_REGION_NAME
access_key = container_config.S3_ACCESS_KEY
secret_key = container_config.S3_SECRET_KEY
remote_db = boto3.client(service_name='s3', region_name=region_name,
                                aws_access_key_id=access_key, aws_secret_access_key=secret_key,)
input_bucket = container_config.S3_INPUT_BUCKET



    
    
if __name__ == '__main__':
    num_processes = 20
    file_num = int(sys.argv[1])
    data_size = int(sys.argv[2]) #328GB=100G 643GB=200G
    
    workflow_name = 'tpcds-16'
    func_name = 'partdd'
    table_parent = 'date_dim'
    table_upload = 'date_dim'
    subprocess.check_output(["./dsdgen", 
                                 "-dir", "./dataset", 
                                 "-table", table_parent, 
                                 "-scale", str(data_size),
                                 "-force"])
    
    file_path = './dataset/{}.dat'.format(table_upload)
    print(file_path)
    for i in range(0,file_num,):
        
        bucket_path = f'{workflow_name}/{file_num}/global-input-{func_name}-{i}.csv'
        remote_db.upload_file(file_path, input_bucket, bucket_path)
        print(bucket_path)
  