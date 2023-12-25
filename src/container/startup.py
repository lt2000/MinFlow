import os
os.system('tc qdisc add dev eth0 root tbf rate 600mbit burst 10mb latency 50ms')
os.system('python3 /proxy/setup.py build')
os.system('python3 /proxy/setup.py install')
os.system("python3 /proxy/proxy.py 1> /proxy/log.txt 2> /proxy/error.txt")