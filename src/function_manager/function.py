import gevent
from gevent import event
from gevent.lock import BoundedSemaphore
import logging
import time
import math
import sys
from container import Container
from function_info import FunctionInfo



# data structure for request info
class RequestInfo:
    def __init__(self, request_id, data):
        self.request_id = request_id
        self.data = data
        self.result = event.AsyncResult()
        self.arrival = time.time()

# manage a function's container pool
class Function:
    mapper_pool = []
    reducer_pool = []
    normal_pool = []
    
    def __init__(self, client, function_info, port_controller):
        self.client = client
        self.info = function_info
        self.port_controller = port_controller

        self.num_processing = 0
        self.rq = []

        # container pool
        self.num_exec = 0 # the number of containers in execution, not in container pool
        self.b = BoundedSemaphore()

    
    # put the request into request queue
    def send_request(self, request_id, runtime, input, output, to, keys):
        data = {'request_id': request_id, 'runtime': runtime, 'input': input, 'output': output, 'to': to,  'keys': keys}
        req = RequestInfo(request_id, data)
        self.rq.append(req)
        res = req.result.get()
        return res

    # receive a request from upper layer
    def dispatch_request(self):
        # no request to dispatch
        if len(self.rq) - self.num_processing == 0:
            return
        self.num_processing += 1
        
        # 1. try to get a workable container from pool
        container = self.self_container()
        
        # create a new container
        while container is None:
        # if container is None:
            container = self.create_container()
           
        # the number of exec container hits limit
        if container is None:
            self.num_processing -= 1
            return

        req = self.rq.pop(0)
        self.num_processing -= 1
        # 2. send request to the container
        logging.info('send request to: %s of: %s, rq len: %d', self.info.function_name, req.request_id, len(self.rq))
        # print(req.data)
        res = container.send_request(req.data)
        req.result.set(res)
        
        # 3. put the container back into pool
        self.put_container(container)
        
        

    # get a container from container pool
    # if there's no container in pool, return None
    def self_container(self):
        res = None
        self.b.acquire()
        if self.info.function_name.startswith('min'):
            if int(self.info.function_name.split('-')[1]): # reduce function
                # print(f'reduce pool len is {Function.reducer_pool}')
                if len(Function.reducer_pool) != 0:
                    logging.info('get container from reducer pool of function: %s, pool size: %d', self.info.function_name, len(Function.reducer_pool))
                    res = Function.reducer_pool.pop(-1)
                    self.num_exec += 1
            else:                                           # map function
                if len(Function.mapper_pool) != 0:
                    logging.info('get container from mapper pool of function: %s, pool size: %d', self.info.function_name, len(Function.mapper_pool))
                    res = Function.mapper_pool.pop(-1)
                    self.num_exec += 1  
        else:
             if len(Function.normal_pool) != 0:
                    logging.info('get container from normal pool of function: %s, pool size: %d', self.info.function_name, len(Function.normal_pool))
                    res = Function.normal_pool.pop(-1)
                    self.num_exec += 1     
        self.b.release()
        return res

    # create a new container
    def create_container(self):
        # do not create new exec container
        # when the number of execs hits the limit
        self.b.acquire() # critical: missing lock may cause infinite container creation under high concurrency scenario
        if self.num_exec + len(Function.reducer_pool) + len(Function.mapper_pool) + len(Function.normal_pool) > self.info.max_containers:
            logging.info('hit container limit, function: %s', self.info.function_name)
            return None
        self.num_exec += 1
        self.b.release()

        logging.info('create container of function: %s', self.info.function_name)
        try:
            # print('break-1: ', gevent.getcurrent())
            container = Container.create(self.client, self.info.img_name, self.port_controller.get(), 'exec')
        except Exception as e:
            print(e)
            self.num_exec -= 1
            return None
        self.init_container(container)
        return container

    # put the container into one of the three pool, according to its attribute
    def put_container(self, container):
        self.b.acquire()
        if self.info.function_name.startswith('min'):
            if int(self.info.function_name.split('-')[1]): # reduce function
                # print(f'put reduer {self.info.function_name}')
                # print(len(Function.reducer_pool))
                Function.reducer_pool.append(container)
                self.num_exec -= 1
            else:                                           # map function
                Function.mapper_pool.append(container)
                self.num_exec -= 1
        else:
            Function.normal_pool.append(container)
            self.num_exec -= 1
        self.b.release()

    # after the destruction of container
    # its port should be give back to port manager
    def remove_container(self, container):
        logging.info('remove container: %s', self.info.function_name)
        container.destroy()
        self.port_controller.put(container.port)

    # do the function specific initialization work
    def init_container(self, container):
        container.init(self.info.workflow_name, self.info.function_name)

    # do the repack and cleaning work regularly
    def repack_and_clean(self):
        # find the old containers
        old_mapper = []
        old_reducer = []
        self.b.acquire()
        Function.mapper_pool = clean_pool(Function.mapper_pool, exec_lifetime, old_mapper)
        Function.reducer_pool = clean_pool(Function.reducer_pool, exec_lifetime, old_reducer)
        Function.normal_pool = clean_pool(Function.normal_pool, exec_lifetime, old_reducer)
        self.b.release()

        # time consuming work is put here
        for c in old_mapper:
            self.remove_container(c)
        for c in old_reducer:
            self.remove_container(c)

def favg(a):
    return math.fsum(a) / len(a)

# life time of three different kinds of containers
exec_lifetime = 60000

# the pool list is in order:
# - at the tail is the hottest containers (most recently used)
# - at the head is the coldest containers (least recently used)
def clean_pool(pool, lifetime, old_container):
    cur_time = time.time()
    idx = -1
    for i, c in enumerate(pool):
        if cur_time - c.lasttime < lifetime:
            idx = i
            break
    # all containers in pool are old, or the pool is empty
    if idx < 0:
        idx = len(pool)
    old_container.extend(pool[:idx])
    return pool[idx:]
