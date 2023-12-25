import boto3
import container_config
import container_config
import multiprocessing
import sys

region_name = container_config.S3_REGION_NAME
access_key = container_config.S3_ACCESS_KEY
secret_key = container_config.S3_SECRET_KEY
remote_db = boto3.client(service_name='s3', region_name=region_name,
                                aws_access_key_id=access_key, aws_secret_access_key=secret_key,)
input_bucket = container_config.S3_INPUT_BUCKET

workflow_name = 'wordcount-shuffle'
      
def upload_data(command,data):
    print(f'file {command} upload start')
    bucket_path = '{}/{}/global-input-{}'.format(workflow_name,file_num,command)
    remote_db.put_object(Body=data, Bucket=input_bucket, Key=bucket_path)
    print(f'file {command} upload end')
    pass

if __name__ == "__main__":

    num_processes = 20
    file_nums = [sys.argv[1]]
    data_size = sys.argv[2] #G
    for file_num in file_nums:
        file_path = 'cleaned_data'  # 替换为实际文件路径
        megabytes_to_read = (data_size * 1000 // file_num) + 1  # 读取10MB的数据
        with open(file_path, 'rb') as file:
            # 每次读取指定数量的字节（1MB = 1024 * 1024字节）
            chunk_size = 1024 * 1024  # 1MB
            total_bytes_read = 0
            data = b''  # 用于存储读取的数据
            while total_bytes_read < megabytes_to_read * 1024 * 1024:
                # 读取数据块
                chunk = file.read(chunk_size)
                if not chunk:
                    break  # 已经读取完整个文件

                # 更新已读取的字节数和数据
                total_bytes_read += len(chunk)
                data += chunk


        # for start in range(0, file_num, num_processes):
        #     greenlets = []
        #     for i in range(start, start + num_processes):
        #         greenlets.append(gevent.spawn(upload_data, i, data))
        #     gevent.joinall(greenlets)
        
        for i in range(file_num):
            upload_data(i, data)


             


    

    


    

