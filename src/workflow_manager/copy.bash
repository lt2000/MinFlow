#!/bin/bash

# 本地目标目录
local_dir="/home/ubuntu/minflow/src/workflow_manager/dataset"

# 远程服务器列表
servers=("ubuntu@node1:/home/ubuntu/dataset/"
         "ubuntu@node2:/home/ubuntu/dataset/"
         "ubuntu@node3:/home/ubuntu/dataset/"
         "ubuntu@node4:/home/ubuntu/dataset/"
         "ubuntu@node5:/home/ubuntu/dataset/"
         "ubuntu@node6:/home/ubuntu/dataset/"
         "ubuntu@node7:/home/ubuntu/dataset/"
         "ubuntu@node8:/home/ubuntu/dataset/"
         "ubuntu@node9:/home/ubuntu/dataset/"
         "ubuntu@node10:/home/ubuntu/dataset/")

# 循环遍历服务器并使用 scp 拷贝文件
for server in "${servers[@]}"; do
    scp -r "$server"* "$local_dir"
    if [ $? -eq 0 ]; then
        echo "拷贝成功：$server"
    else
        echo "拷贝失败：$server"
    fi
done

echo "拷贝完成"
