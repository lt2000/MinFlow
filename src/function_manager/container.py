import gevent
import requests
import sys
import docker
import time
from docker.types import Mount
import os
sys.path.append('../../config')
import config

base_url = 'http://127.0.0.1:{}/{}'


class Container:
    # create a new container and return the wrapper
    @classmethod
    def create(cls, client, image_name, port, attr):
        
        host_path = "/home/ubuntu/ramdisk"
        container_path = "/mnt/ramdisk"
        volumes = {host_path: {'bind': container_path, 'mode': 'rw'}}
        
        container = client.containers.run(image_name,
                                          detach=True,
                                          cap_add = "NET_ADMIN",
                                          ports={'5000/tcp': str(port)},
                                          labels=['workflow'],
                                          volumes=volumes)

        # Get the veth of the container
        time.sleep(1)
        container.reload()
        # First get the pid of the container process
        CON_PID = container.attrs['State']['Pid']
        # First get the namespace directory of the container
        CON_NET_SANDBOX = container.attrs['NetworkSettings']['SandboxKey']
        # get the name of container 
        CON_NAME = container.attrs['Name']
        # Create a link to the container network namespace in the netns directory 
        # so that you can run the ip netns command on the docker host to perform 
        # operations on the container network namespace
        os.system('sudo rm -f /var/run/netns/{}'.format(CON_PID))
        os.system('sudo mkdir -p /var/run/netns')
        os.system(
            'sudo ln -s {} /var/run/netns/{}'.format(CON_NET_SANDBOX, CON_PID))
        # Obtain the virtual NIC ID of the host
        VETH_ID = os.popen('sudo ip netns exec {} ip link show eth0 | head -n 1 | awk -F: {}'.format(
            CON_PID, '\'{print $1}\'')).read().strip()
        # Obtain the name of the virtual NIC on the host
        VETH_NAME = os.popen('sudo ip link|grep "if{}:"|awk {}|awk -F@ {}'.format(
            VETH_ID, '\'{print $2}\'', '\'{print $1}\'')).read().strip()
        # Finally, delete the link created in the netns directory
        os.system('sudo rm -f /var/run/netns/{}'.format(CON_PID))

        # limit docker0 veth upload and download bandwidth
        os.system('sudo tc qdisc add dev {} root tbf rate 600mbit burst 10mb latency 50ms'.format(VETH_NAME))
        
        # connect container to network bridge docker1
        os.system('sudo docker network connect docker1 {}'.format(CON_NAME))
        res = cls(container, port, attr)
        
        time.sleep(3)
        res.wait_start()
        return res

    # get the wrapper of an existed container
    # container_id is the container's docker id
    @classmethod
    def inherit(cls, client, container_id, port, attr):
        container = client.containers.get(container_id)
        return cls(container, port, attr)

    def __init__(self, container, port, attr):
        self.container = container
        self.port = port
        self.attr = attr
        self.lasttime = time.time()

    # wait for the container cold start
    
    def wait_start(self):
        # count = 0
        while True:
            try:
                # count += 1
                r = requests.get(base_url.format(self.port, 'status'))
                # print(gevent.getcurrent(), 'little')
                if r.status_code == 200:
                    break    
            except Exception as e:
                pass
            # gevent.sleep(0.005)
            

    # send a request to container and wait for result
    def send_request(self, data={}):
        # print(base_url.format(self.port, 'run'))
        r = requests.post(base_url.format(self.port, 'run'), json=data)
        self.lasttime = time.time()
        return r.json()

    # initialize the container
    def init(self, workflow_name, function_name):
        data = {'workflow': workflow_name, 'function': function_name}
        r = requests.post(base_url.format(self.port, 'init'), json=data)
        self.lasttime = time.time()
        return r.status_code == 200

    # kill and remove the container
    def destroy(self):
        self.container.remove(force=True)
