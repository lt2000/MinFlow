import couchdb
import time
import subprocess
time.sleep(1)
hostname = subprocess.check_output(f'hostname -I| awk \'{{print $1}}\'',shell=True)
couchdb_url = 'http://little:little@{}:5984/'.format(hostname.strip().decode())

print(couchdb_url)
db = couchdb.Server(couchdb_url)
db.create('workflow_latency')
db.create('results')
db.create('log')
