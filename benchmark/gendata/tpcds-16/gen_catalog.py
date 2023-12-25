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

table_upload = ['catalog_sales', 'catalog_returns']
workflow_name = 'tpcds-16'
func_name = ['partcs', 'partcr']

def gen_data(command):
    subprocess.check_output(command)
    
    for idx, table in enumerate(table_upload):
        file_path = './dataset/{}_{}_{}.dat'.format(table,command[-1],file_num)
        bucket_path = '{}/{}/global-input-{}-{}.csv'.format(workflow_name,file_num,func_name[idx],int(command[-1])-1)
        print(file_path)
        remote_db.upload_file(file_path, input_bucket, bucket_path)
        print(bucket_path)
        pass
    
    
    
if __name__ == '__main__':
    num_processes = 20
    file_num = int(sys.argv[1])
    data_size = int(sys.argv[2]) #328GB=100G 643GB=200G
    
    table_parent = 'catalog_sales'
    for start in range(1, file_num + 1, num_processes):
        commands = []
        for i in range(start, start + num_processes):
            command = ["./dsdgen", 
                       "-dir", "./dataset", 
                       "-table", table_parent, 
                       "-scale", str(data_size),
                       "-force",
                       "-parallel", str(file_num),
                       "-child", str(i)]
            commands.append(command)
        
        with multiprocessing.Pool(processes=num_processes) as pool:
            results = pool.map(gen_data, commands)
             
        os.system(f'sudo rm -r ./dataset/*')
