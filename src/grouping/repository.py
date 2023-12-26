import couchdb
import redis
from typing import Dict, List
import sys
import subprocess

sys.path.append('../../config')
import config


class Repository:
    def __init__(self, workflow_name, node_ip, remove_old_db=True):
        self.redis = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
        couchdb_url = 'http://little:little@{}:5984/'.format(node_ip)
        # print(couchdb_url)
        self.couch = couchdb.Server(couchdb_url)
        if remove_old_db:
            db_list = [workflow_name + '_function_info', workflow_name + '_function_info_raw', workflow_name + '_workflow_metadata']
            for db_name in db_list:
                if db_name in self.couch:
                    self.couch.delete(db_name)

    def save_function_info(self, function_info, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        for name in function_info:
            db[name] = function_info[name]
    
    def save_bundling_info(self, bundling_info, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save({'bundling_info': bundling_info})
    
    def save_foreach_info(self, foreach_info, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save({'foreach_info': foreach_info})

    def save_foreach_functions(self, foreach_functions, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save({'foreach_functions': list(foreach_functions)})
    
    def save_min_functions(self, min_functions, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save({'min_functions': list(min_functions)})

    def save_bundling_functions(self, bundling_functions, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save({'bundling_functions': list(bundling_functions)})

    def save_critical_path_functions(self, critical_path_functions, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save({'critical_path_functions': list(critical_path_functions)})

    def save_all_addrs(self, addrs, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save({'addrs': list(addrs)})

    def save_start_functions(self, start_functions, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save({'start_functions': start_functions})

    def save_basic_input(self, basic_input, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save(basic_input)

    def fetch_finished_request_id(self, workflow_name: str) -> List[str]:
        db = self.couch['log']
        mango = {'selector': {'workflow': workflow_name, 'status': 'FINISH'}}
        return [row['request_id'] for row in db.find(mango)]

    def fetch_logs(self, workflow_name: str, request_id: str) -> List[Dict]:
        db = self.couch['log']
        mango = {'selector': {'request_id': request_id}}
        result = [dict(row) for row in db.find(mango)]
        result.remove({'request_id': request_id, 'workflow': workflow_name, 'status': 'EXECUTE'})
        result.remove({'request_id': request_id, 'workflow': workflow_name, 'status': 'FINISH'})
        return result
    
    def remove_logs(self, request_id: str):
        db = self.couch['log']
        mango = {'selector': {'request_id': request_id}}
        for row in db.find(mango):
            db.delete(row)
