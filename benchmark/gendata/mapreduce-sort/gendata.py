import subprocess
import pickle
import boto3
import container_config
import time
import sys


region_name = container_config.S3_REGION_NAME
access_key = container_config.S3_ACCESS_KEY
secret_key = container_config.S3_SECRET_KEY
remote_db_server = boto3.resource(service_name='s3', region_name=region_name,
                                aws_access_key_id=access_key, aws_secret_access_key=secret_key,)


def put_func(k,v):
    try:
        remote_db.put_object(Key=k, Body=v)
        print(k)
        return 0
    except Exception as e:
        return 1

def main(func_idx, func_num, total_size):
    

    num_of_records = total_size // func_num
    # generate data
    begin = func_idx*num_of_records
    data = subprocess.check_output(["./gensort",
                                "-b"+str(begin),
                                str(num_of_records),
                                "/dev/stdout"])
  
    start = time.time()
    k = 'mapreduce-sort/{}/global-input-'.format(func_num) + str(func_idx)
    remote_db.put_object(Key=k, Body=pickle.dumps(data))
    print(k)
    end = time.time()
    print(end - start)
    
if __name__ == '__main__':
    func_nums = [int(sys.argv[1])]
    bucket_list = [container_config.S3_INPUT_BUCKET]
    record_num = int(sys.argv[2])*10*1000
    total_size = [record_num] 
    
    for k,v in enumerate(bucket_list):
        remote_db = remote_db_server.Bucket(v)
        for func_num in func_nums:
            for i in range(func_num):
                print('func:', i)
                main(i, func_num, total_size[k])
    
        
    
    


