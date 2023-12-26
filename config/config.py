REDIS_HOST = '172.16.0.1' # it serves to connect with the local redis via docker1, so it should be '172.16.0.1'
REDIS_PORT = 8002 # it follows the same configuration as created redis by docker (e.g., -p 6379:6379)
REDIS_DB = 0
GATEWAY_ADDR = '172.31.42.166:7001' # need to update as your private_ip
WORKFLOW_YAML_ADDR = {'wordcount-shuffle': '/home/ubuntu/minflow/benchmark/wordcount-shuffle/flat_workflow.yaml', 
                      'mapreduce-sort': '/home/ubuntu/minflow/benchmark/mapreduce-sort/flat_workflow.yaml',
                      'tpcds-16': '/home/ubuntu/minflow/benchmark/tpcds-16/flat_workflow.yaml'}
NETWORK_BANDWIDTH = 25 * 1024 * 1024 / 4 # 25MB/s / 4
NETWORK_BANDWIDTH_LIMIT = 2048 # kbps
NET_MEM_BANDWIDTH_RATIO = 15 # mem_time = net_time / 15
CONTAINER_MEM = 256 * 1024 * 1024 # 256MB
NODE_MEM = 128 * 1024 * 1024 * 1024 # 256G
RESERVED_MEM_PERCENTAGE = 0.2
GROUP_LIMIT = 200

FUNCTION_INFO_ADDRS = {'mapreduce-sort': '../../benchmark/mapreduce-sort'}
# FUNCTION_INFO_ADDRS = {'wordcount-shuffle': '../../benchmark/wordcount-shuffle'}
# FUNCTION_INFO_ADDRS = {'tpcds-16': '../../benchmark/tpcds-16'}
SHUFFLE_MODE = 'min' # single, min
DATA_MODE = 'optimized' # raw, optimized
CONTROL_MODE = 'WorkerSP' # WorkerSP, MasterSP
CLEAR_DB_AND_MEM = True
MODELER = False
LAMBADA_OPT = False
BALANCE_STATISTICS = False

# S3 ACESS
S3_ACCESS_KEY = ''
S3_SECRET_KEY = ''
S3_REGION_NAME = 'cn-northwest-1'
S3_INPUT_BUCKET = 'minflow-myinput'
S3_INTER_BUCKET = 'minflow-myresult'


# MODELER PARAMETERS
S3_WRITE_QPS_LIMIT = 3500 # 3k/s
S3_READ_QPS_LIMIT = 5500 # 5k/s
FUNC_BANDWIDTH_LIMIT = 599552 # 75MB/s
REDIS_BANDWIDTH_LIMIT = 1000000 # MB
REDIS_QPS_LIMIT = 1000000
NODE_NUM = 10
INPUT_DATA_SIZE = 200000 # MB
INTER_INPUT_RATIO = {'mapreduce-sort': 1, 'tpcds-16': 1, 'wordcount-shuffle': 0.2}
WORKFLOW_NAME = 'mapreduce-sort'



