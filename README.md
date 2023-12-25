## Introduction

MinFlow is a serverless workflow engine that achieves high-performance and cost-efficient data passing for I/O-intensive stateful serverless analytics.

## Hardware Depedencies and Private IP Address

1. In our experiment setup, we use 10 AWS EC2 instances installed with Ubuntu 22.04 LTS (m6i.24xlarge , cores: 96, DRAM: 384GB) for each node. 

2. Please select a node as the master node to genetate execution plans and save the private IP address of the master node as the **<master_ip>**, and save the private IP address of the other 9 worker nodes as the **<worker_ip>**. We recommend using `sshfs` to mount the source code on the master node to other nodes in order to update the code synchronously.

## About Config Setting

There are 3 places for config setting and we  introduce parameters that need to be updated. 

1. `src/container/container_config.py` specifies the following configs:

   * Account information of AWS S3
     * S3_ACCESS_KEY
     * S3_SECRET_KEY
     * S3_REGION_NAME
     * S3_INPUT_BUCKET (Store input data)
     * S3_INTER_BUCKET (Store intermediate data and output data)
   * The number of node
     * NODE_NUM = 10

2. `src/grouping/node_info.ymal` specifies worker node ip address.

3. All other configurations are in `config/config.py`.

   * Ip configs

     * GATEWAY_ADDR = '172.31.34.109:7001' (**need to be updated as your master private_ip**)
     * MASTER_HOST = '172.31.34.109:8001' (**need to be updated as your master private_ip**)

    * method configs 

      * SHUFFLE_MODE = 'min' # single, min
      * DATA_MODE = 'optimized' # raw, optimized
      * MODELER = True
      * LAMBADA_OPT = True
      * BALANCE_STATISTICS = False

   * AWS S3 configs

     * S3_ACCESS_KEY
     * S3_SECRET_KEY 

     * S3_REGION_NAME

     * S3_INPUT_BUCKET (Store input data)

     * S3_INTER_BUCKET (Store intermediate data and output data)

   * Modeler configs

     * NODE_NUM = 10
     * WORKFLOW_NAME = 'mapreduce-sort'

## Installation and Software Dependencies

Clone our code `https://github.com/lt2000/MinFlow.git` and Perform the following four steps on each node:

1. Python

   * We use Anaconda manage packages, and the python version is 3.9
   * Run `scripts/python_install.bash` to install packages.

2. Docker and database

   * We utilize Docker for container management, while employing CouchDB and Redis as metadata and log databases, respectively.
   * To install docker and those database, you need to run `scripts/docker_install.bash` 

3. Build docker images

   * Run `benchmark/mapreduce-sort/create_image.sh`, `benchmark/tpcds-16/create_image.sh`, `benchmark/wordcount-shuffle/create_image.sh` to build image for Terasort, TPC-DS-Q16, WordCount,  respectively.

4. Mount `tmpfs` as local storage

   * ```
     sudo mkdir /home/ubuntu/ramdisk/
     sudo mount -t tmpfs -o size=30G tmpfs /home/ubuntu/ramdisk/
     ulimit -n 524288
     ```

Generate execution plans for different method on the master node:

1. Baseline

   * Change the configuration by `SHUFFLE_MODE = single`  and `DATA_MODE = raw` in `src/config/config.py`

   * Run `src/grouping/metadata.py`

     ```bash
     python metadata.py <function_number> <workflow_name>
     e.g.,
     python metadata.py 600 mapreduce-sort
     python metadata.py 400 tpcds-16
     python metadata.py 200 wordcount-shuffle
     ```

2. FaaSFlow

   * Change the configuration by `SHUFFLE_MODE = single`  and `DATA_MODE = optimized` in `src/config/config.py`
   * Run `src/grouping/metadata.py`

3. Lambada

   * Change the configuration by `SHUFFLE_MODE = min`  and `DATA_MODE = raw` in `src/config/config.py`
   * Run `src/grouping/metadata.py`

4. MinFlow

   * Change the configuration by `SHUFFLE_MODE = min`  and `DATA_MODE = optimized` in `src/config/config.py`
   * Run `src/grouping/metadata.py`

## Generate Input Data and Upload to S3

Since Terasort benchmark is easier to generate input data than TPC-DS-Q16 and WordCount benchmark, we recommend that AEC first use Terasort benchmark to reproduce our results.

1. Terasort

   * Change the configuration by your AWS S3 account infomation in `benchmark/gendata/mapreduce-sort/container_config.py`

   * Run `benchmark/gendata/mapreduce-sort/gendata.py`

     ```bash
     python gendata.py <function_number> <data_size(MB)>
     e.g.,
     python gendata.py 400 200000
     ```

2. TPC-DS-Q16

   * Change the configuration by your AWS S3 account infomation in `benchmark/gendata/tpcds-16/container_config.py`

   * We need to generate the five tables involved in TPC-DS-Q16

     * data_dim

       ```bash
       python gen_date_dim.py <function_number> <data_size(GB)>
       e.g.,
       python gen_date_dim.py 400 328
       ```

       It should be noted that 328GB is the total size of all tables in TPC-DS, and the corresponding TPC-DS-Q16 involves five tables with a size of 100G, similarly, 643GB=200G

     * call_center

       ```bash
       python gen_call_center.py <function_number> <data_size(GB)>
       e.g.,
       python gen_call_center.py 400 328
       ```

     * customer_address

       ```
       python gen_customer_address.py <function_number> <data_size(GB)>
       e.g.,
       python gen_call_center.py 400 328
       ```

     * catalog_sales and catalog_returns

       ```
       python gen_catalog.py <function_number> <data_size(GB)>
       e.g.,
       python gen_call_center.py 400 328
       ```

3. WordCount

   * Change the configuration by your AWS S3 account infomation in `benchmark/gendata/wordcount-shuffle/container_config.py`

   * Download Wiki dataset from https://engineering.purdue.edu/~puma/datasets.htm

   * Run `benchmark/gendata/wordcount-shuffle/clean.py` to replace non-word characters with Spaces

   * Run `benchmark/gendata/wordcount-shuffle/gendata.py`

     ```bash
     python gendata.py <function_number> <data_size(GB)>
     e.g.,
     python gendata.py 400 100
     ```

## "Hello world"-sized example

We will demonstrate how to execute MinFlow with a "Hello world"-sized example, i.e., 8 mapper x 8 reducer  under 8MB Terasort benchamrk.

1. Generate input data

   ```
   cd benchmark/gendata/mapreduce-sort/
   python gendata.py 8 8
   ```

2. Change the configurations

   * `config/config.py`: 

     * FUNCTION_INFO_ADDRS = {'mapreduce-sort': '../../benchmark/mapreduce-sort'}
     * SHUFFLE_MODE = 'min' # single, min
     * DATA_MODE = 'optimized' # raw, optimized
     * MODELER = True
     * WORKFLOW_NAME = 'mapreduce-sort'

   * `/test/asplos/data_overhead/run.py`
     * workflow_pool = ['mapreduce-sort']

3. Generate execution plans 

   * Run `src/grouping/metadata.py` on the master node

     ```
     python metadata.py 8 mapreduce-sort
     ```

4. Start the engine proxy and gateway

   * Enter `src/workflow_manager` and start the engine proxy with the local  <worker_ip> on each node by the following <span id="jump">command</span>: 

     ```
     python3 proxy.py <worker_ip> 8001             (proxy start)
     ```

   * Then enter `src/workflow_manager` and start the gateway on the master node by the following command: 

     ```
     python3 gateway.py <master_ip> 7001           (gateway start)
     ```

5. Run the workflow

   * Enter `test/fast/data_overhead` and run the following <span id="jump">command</span>: 

     ```
     python run.py --split_ratio=8 --method=3
     #method number: Baseline=0,FaaSFlow=1,Lambada=2,MinFlow=3
     ```

   * The job completion time is written to `mapreduce-sort_request.json`

     It should be noted that the first run is the cold start of the container, and only the results after the second run are counted

## Run Experiment

We will use 200G Terasort under 600 function parallelism running on Minflow as an example to show how to reproduce our main experiments

### Shuffle Time (Section 4.2)

1. Generate input data

   ```
   cd benchmark/gendata/mapreduce-sort/
   python gendata.py 600 200000
   ```

2. Change the configurations

   * `config/config.py`: 
     * FUNCTION_INFO_ADDRS = {'mapreduce-sort': '../../benchmark/mapreduce-sort'}
     * SHUFFLE_MODE = 'min' # single, min
     * DATA_MODE = 'optimized' # raw, optimized
     * MODELER = True
     * WORKFLOW_NAME = 'mapreduce-sort'
   * `/test/asplos/data_overhead/run.py`
     * workflow_pool = ['mapreduce-sort']

3. Generate execution plans 

   * Run `src/grouping/metadata.py` on the master node

     ```
     python metadata.py 600 mapreduce-sort
     ```

4. Start the engine proxy and gateway

   * Enter `src/workflow_manager` and start the engine proxy with the local  <worker_ip> on each node by the following <span id="jump">command</span>: 

     ```
     python3 proxy.py <worker_ip> 8001             (proxy start)
     ```

   * Then enter `src/workflow_manager` and start the gateway on the master node by the following command: 

     ```
     python3 gateway.py <master_ip> 7001           (gateway start)
     ```

5. Run the workflow

   * Enter `test/fast/data_overhead` and run the following <span id="jump">command</span>: 

     ```
     python run.py --split_ratio=600 --method=3
     #method number: Baseline=0,FaaSFlow=1,Lambada=2,MinFlow=3
     ```

   * The job completion time is written to `mapreduce-sort_request.json`

6. Calculate the shuffle time

   * We breakdown the job completion time into three parts: in/output time, computing time, and shuffle time. Each function autonomously records its respective breakdown of execution time to the Redis instance residing on the node where it is executed. We aggregate breakdown results from all nodes onto the master node for the purpose of calculating the shuffle time for the job. The detailed operations are as follows:

   * Copy breakdown results

     * Change the configurations by `hostlist=[nodes ip]` in `/test/breakdown/copydata.py`

     * ```
       python copydata.py
       ```

     * Copy the file `mapreduce-sort_request.json` obtained in step 5 to `/test/breakdown`

   * Calculate the in/output time, computing time, and shuffle time

     * Enter `/test/breakdown`

     * ```
       python overall_break_critical.py mapreduce-sort 600
       ```

     * The results will be printed on the terminal

### Load Balance (Section 4.2)

At intervals of 50 milliseconds, we collect statistics on each node's CPU utilization, memory utilization, and network send and receive throughput. To obtain statistical information, the details are as follows:

1. Change configurations by `BALANCE_STATISTICS=True` in `config/config.py`

2. * Change configurations by `node_dict={nodes ip}` in `/src/workflow_manager/resource.py`
   * Change configurations by `path=/path/to/statistics` in `/src/workflow_manager/resource.py`

3. Copy the statistics of all nodes to `/test/load_balance/dataset` of the master node

   * Enter `/test/load_balance` and change configurations by `workflow_name=request_id_prefix` in `/test/load_balance/balance.py`

   * ```
     python balance.py
     ```

   * Output four figures:`cpu.png`, `mem.png`, `sent.png`, `resv.png`

### Overall Time (Section 4.3)

Similar to Shuffle Time (Section 4.2), the overall time is stored in file  `mapreduce-sort_request.json`.

### Technique Breakdown (Section 4.4)

We progressively integrate the three components (i.e., Topology optimizer, Function scheduler, Configuration modeler) to show their respective contribution to MinFlow’s shuffle time reduction.

1. Topology optimizer
   * Change the configurations by `SHUFFLE_MODE = 'min'` , `DATA_MODE = 'raw'`, `LAMBADA_OPT = False`
   * Run the workflow as Shuffle Time (Section 4.2)
2. Topology optimizer + Function scheduler
   * Change the configurations by `SHUFFLE_MODE = 'min'` , `DATA_MODE = 'optimized'`, `LAMBADA_OPT = False`
   * Run the workflow as Shuffle Time (Section 4.2)
3. Topology optimizer + Function scheduler + Configuration modeler
   * Change the configurations by `SHUFFLE_MODE = 'min'` , `DATA_MODE = 'optimized'`, `MODELER = True`
   * Run the workflow as Shuffle Time (Section 4.2)

### Scalability (Section 4.4)

Evaluate the impact on overhead of Topology optimizer and Function scheduler with increasing function parallelism (from 1 to 1000).

1. Enter `/test/scalability` and run `scalability.py`

   ```
   python scalability.py
   ```

   Output two files: ay.pickle, by.pickle

2. Run `/test/scalability/plot.py`

   ```
   python plot.py
   ```

   Output a figure: scalability.png

### Various input size and Tunable parallelism (Section 4.5)

Similar to Shuffle Time (Section 4.2)