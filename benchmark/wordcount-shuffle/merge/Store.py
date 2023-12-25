import gevent
import json
import os
import time
import container_config
from botocore.exceptions import ClientError



class Store:
    def __init__(self, workflow_name, function_name, request_id, input, output, to, keys, 
                 runtime, redis_log_server, remote_db_server, phase):
        # to: where to store for outputs
        # keys: foreach key (split_key) specified by workflow_manager
        self.remote_db = remote_db_server
        self.ramdisk = '/mnt/ramdisk/'
        self.redis_log = redis_log_server
        self.fetch_dict = {}
        self.put_dict = {}
        self.workflow_name = workflow_name
        self.function_name = function_name
        self.request_id = request_id
        self.input = input
        self.output = output
        self.to = to
        self.keys = keys
        self.runtime = runtime
        self.phase = phase
        self.global_input = ''
        self.node = container_config.NODE_NUM
        self.bundling_size = keys['bundling_size']
        self.db_mem = {}
        if os.path.exists('work'):
            os.system('rm -rf work')
        os.mkdir('work')

    def clear(self):
        self.fetch_dict = {}
        self.put_dict = {}  
        self.global_input = ''       
          
    def gen_path(self, k):
        return self.request_id + '/' + k

    def fetch_io_keys(self, k):
        return self.keys[k]

    def fetch_from_mem(self, k, redis_key, content_type):
        file_path = self.ramdisk + redis_key 
        if content_type == 'application/json':
            with open(file_path, 'rb') as f:
                raw_redis_value = f.read()
            redis_value = raw_redis_value.decode()
            os.remove(file_path)
            self.fetch_dict[k] = json.loads(redis_value)
        else:
            try:
                with open(file_path, 'rb') as f:
                    self.fetch_dict[k] = f.read()
                os.remove(file_path)
            except Exception as e:
                print(f'error is {e}')

    def fetch_from_db_wrapper(self, k):
        try:  # Binary file
            filename = k + '.json'
            s3_ob = self.remote_db.get_object(Bucket=container_config.S3_INTER_BUCKET, Key=self.gen_path(filename))
            response = s3_ob['Body']
            b = response.read()
            self.fetch_dict[k] = json.loads(b)
            return 0
        except ClientError as e:  
            if e.response['Error']['Code'] == 'NoSuchKey':
                # print("The specified key does not exist.",flush=True)
                self.fetch_dict[k] = 'NoSuchKey'
                return 0
            else:
                return 1
        
        
                    
    def fetch_from_db(self, k):
        while (self.fetch_from_db_wrapper(k)) == 1:
            pass

    def fetch_global_input_wrapper(self,k):
        split_ratio = self.keys['split_ratio']
        try:  # Binary file
            s3_ob = self.remote_db.get_object(Bucket=container_config.S3_INPUT_BUCKET, Key=self.workflow_name + '/{}/'.format(split_ratio) + k)
            response = s3_ob['Body']
            self.global_input = response.read()
        except ClientError as e:  # JSON file
            if e.response['Error']['Code'] == 'NoSuchKey':
                print("The specified key does not exist.")
                filename = k + '.json'
                try:
                    s3_ob = self.remote_db.get_object(Bucket=container_config.S3_INPUT_BUCKET, Key=self.workflow_name + '/{}/'.format(split_ratio) + filename)
                    response = s3_ob['Body']
                    b = response.read()
                    self.global_input = json.loads(b)
                except ClientError as e:
                    self.global_input = 'NoSuchKey'
                    
    def fetch_global_input(self, func_name='', suffix=''):
        self.fetch_dict = {}
        cur_func = self.keys['output'][0].split('_')[0]
        if func_name=='' and suffix=='':
            name_split = cur_func.split('-')
            k = 'global-input-{}'.format(name_split[-1])
        else:
            name_split = cur_func.split('-')
            k = 'global-input-{}-{}{}'.format(func_name,name_split[-1],suffix)
            
        greenlet = gevent.spawn(self.fetch_global_input_wrapper, k)
        gevent.joinall([greenlet])
        
        return self.global_input

    
    # input_keys: specify the keys you want
    def fetch(self, input_keys):
        self.fetch_dict = {}
        
        greenlets = []
        for key in input_keys:
            greenlets.append(gevent.spawn(self.fetch_wrapper, key))
        gevent.joinall(greenlets)
       
        return self.fetch_dict

    def fetch_wrapper(self, k):
        redis_key_1 = self.request_id + '_' + k
        redis_key_2 = self.request_id + '_' + k + '.json'


        if os.path.exists(self.ramdisk + redis_key_1):
            self.fetch_from_mem(k, redis_key_1, 'bytes')
        elif os.path.exists(self.ramdisk + redis_key_2):
            self.fetch_from_mem(k, redis_key_2, 'application/json')
        else:  # if not
            self.fetch_from_db(k)
        
            

    def put_to_mem(self, k, content_type):
        if content_type == 'application/json':
            redis_key = self.request_id + '_' + k + '.json'
            file_path = self.ramdisk + redis_key
            with open(file_path, 'w') as f:
                f.write(self.put_dict[k])
        else:
            redis_key = self.request_id + '_' + k
            file_path = self.ramdisk + redis_key
            with open(file_path, 'wb') as f:
                f.write(self.put_dict[k])
        


    def put_to_db_wrapper(self, k, content_type):
        try:
            filename = k
            if content_type == 'application/json':
                filename = filename + '.json'
            self.remote_db.put_object(Bucket=container_config.S3_INTER_BUCKET, 
                                        Key=self.gen_path(filename), Body=self.put_dict[k])
            return 0
        except Exception as e:
            return 1

    def put_to_db(self, k, content_type):
        while (self.put_to_db_wrapper(k, content_type)) == 1:
            pass

    # output_result: {'k1': ...(dict-like), 'k2': ...(byte stream)}
    # output_content_type: default application/json, just specify one when you need to
    
    def put(self, output_result, output_content_type):
        for k in output_result:
            if k not in output_content_type:
                # default: dict-like, should be stored in json style
                output_content_type[k] = 'application/json'
        self.put_dict = output_result

        for k in output_result:
            if output_content_type[k] == 'application/json':
                self.put_dict[k] = json.dumps(self.put_dict[k])
   
        greenlets = []
        for key in output_content_type:
            greenlets.append(gevent.spawn(self.put_wrapper, key, output_content_type[key]))
        gevent.joinall(greenlets)

    def put_wrapper(self, k, output_content_type):
        if self.to == 'DB+MEM':
            if self.db_mem[k]: 
                self.put_to_mem(k, output_content_type)
            else:
                self.put_to_db(k, output_content_type)  
                   
        elif self.to == 'DB':
            self.put_to_db(k, output_content_type) 
             
        elif self.to == 'MEM':
            self.put_to_mem(k, output_content_type)
   
        
    def log_save(self, key,vaule):
        self.redis_log.set(key, vaule)
        
    def log(self, key,vaule):
        redis_key = self.request_id + '_' + key
        greenlet = gevent.spawn(self.log_save, redis_key, vaule)
        gevent.joinall([greenlet])

