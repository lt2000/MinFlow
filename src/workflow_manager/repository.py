from typing import Any, List
import couchdb
import redis
import json
import sys
import boto3
import subprocess
sys.path.append('../../config')
import config
import os


hostname = subprocess.check_output(f'hostname -I| awk \'{{print $1}}\'',shell=True)
couchdb_url = 'http://little:little@{}:5984/'.format(hostname.strip().decode())
workflow_name = config.WORKFLOW_NAME
db_list = [workflow_name + '_function_info', workflow_name + '_function_info_raw', workflow_name + '_workflow_metadata']

class Repository:
    def __init__(self):
        self.redis = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
        self.couch = couchdb.Server(couchdb_url)
        self.db = {}
        for database in db_list:
            db = self.couch[database]
            all_docs = [doc for doc in db.view('_all_docs', include_docs=True)]
            self.db[database] = [doc['doc'] for doc in all_docs]
    
    # get all function_name for every node seems to solve the problem of KeyError Exception in manager.py, line 103
    def get_current_node_functions(self, ip: str, mode: str) -> List[str]:
        db = self.db[mode]
        functions = []
        for item in db:
            functions.append(item['function_name'])
        return functions
    
    def get_bundling_info(self, db_name) -> List[str]:
        db = self.db[db_name]
        for item in db:
            if 'bundling_info' in item:
                return item['bundling_info']
    
    def get_foreach_info(self, db_name) -> List[str]:
        db = self.db[db_name]
        for item in db:
            if 'foreach_info' in item:
                return item['foreach_info']

    def get_foreach_functions(self, db_name) -> List[str]:
        db = self.db[db_name]
        for item in db:
            if 'foreach_functions' in item:
                return item['foreach_functions']

    def get_min_functions(self, db_name) -> List[str]:
        db = self.db[db_name]
        for item in db:
            if 'min_functions' in item:
                return item['min_functions']
            
    def get_bundling_functions(self, db_name) -> List[str]:
        db = self.db[db_name]
        for item in db:
            if 'bundling_functions' in item:
                return item['bundling_functions']
            
    def get_start_functions(self, db_name) -> List[str]:
        db = self.db[db_name]
        for item in db:
            if 'start_functions' in item:
                return item['start_functions']

    def get_all_addrs(self, db_name) -> List[str]:
        db = self.db[db_name]
        for item in db:
            if 'addrs' in item:
                return item['addrs']

    def get_function_info(self, function_name: str, mode: str) -> Any:
        db = self.db[mode]
        for item in db:
            if item['function_name'] == function_name:
                return item


    def create_request_doc(self, request_id: str) -> None:
        if request_id in self.couch['results']:
            doc = self.couch['results'][request_id]
            self.couch['results'].delete(doc)
        self.couch['results'][request_id] = {}

    # def create_request_s3_doc(self, request_id: str) -> None:
    #     bucket = self.s3.Bucket('little-results')
    #     folder_key = request_id + '/'
    #     objects_to_delete = bucket.objects.filter(Prefix=folder_key)
    #     if len(list(objects_to_delete)) != 0:
    #         bucket.delete_objects(
    #             Delete={
    #                 'Objects': [{'Key': obj.key} for obj in objects_to_delete]
    #             })
    #     bucket.put_object(Key=folder_key)

    def get_keys(self, request_id: str) -> Any:
        keys = dict()
        doc = self.couch['results'][request_id]
        for k in doc:
            if k != '_id' and k != '_rev' and k != '_attachments':
                keys[k] = doc[k]
        return keys

    # fetch result from couchdb/redis
    def fetch_from_mem(self, redis_key, content_type):
        if content_type == 'application/json':
            redis_value = self.redis[redis_key].decode()
            return json.loads(redis_value)
        else:
            return self.redis[redis_key]

    def fetch_from_db(self, request_id, key):
        db = self.couch['results']
        f = db.get_attachment(request_id, filename=key, default='no attachment')
        if f != 'no attachment':
            return f.read()
        else:
            filename = key + '.json'
            f = db.get_attachment(request_id, filename=filename, default='no attachment')
            return json.load(f)

    def fetch(self, request_id, key):
        print('fetching...', key)
        redis_key_1 = request_id + '_' + key
        redis_key_2 = request_id + '_' + key + '.json'
        value = None
        if redis_key_1 in self.redis:
            value = self.fetch_from_mem(redis_key_1, 'bytes')
        elif redis_key_2 in self.redis:
            value = self.fetch_from_mem(redis_key_2, 'application/json')
        else:  # if not
            value = self.fetch_from_db(request_id, key)
        print('fetched value: ', value)
        return value
    
    def clear_mem(self, request_id):
        keys = self.redis.keys()
        for key in keys:
            key_str = key.decode()
            if key_str.startswith(request_id):
                self.redis.delete(key)

    def clear_db(self, request_id):
        # db = self.couch['results']
        # db.delete(db[request_id])
        pass

    def log_status(self, workflow_name, request_id, status):
        log_db = self.couch['log']
        log_db.save({'request_id': request_id, 'workflow': workflow_name, 'status': status})
    
    def save_latency(self, log):
        latency_db = self.couch['workflow_latency']
        latency_db.save(log)