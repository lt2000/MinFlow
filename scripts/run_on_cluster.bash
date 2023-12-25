#!/bin/bash

# 集群节点的SSH用户名和IP地址
USERNAME="ubuntu"
NODES=("node1_ip" "node2_ip" "node3_ip")

# 启动命令
START_COMMAND="./start_application.sh"

# 遍历集群节点并执行启动命令
for NODE in "${NODES[@]}"; do
    ssh "$USERNAME@$NODE" "$START_COMMAND"
done
