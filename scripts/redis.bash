#!/bin/bash

docker restart couchdb
docker rm -f redis
docker run -itd -p 8002:6379 --name redis  --network docker1 redis

# # 创建八个 Redis 容器
# for i in {0..15}
# do
#   # 设置容器名称
#   container_name="redis-${i}"

#   # 设置端口号
#   port=$((8002 + $i))

  

#   # #remove
#   docker rm -f "$container_name"

#   # # #restart
#   # docker restart "$container_name"

# done

# for i in {0..7}
# do
#   # 设置容器名称
#   container_name="redis-${i}"

#   # 设置端口号
#   port=$((8002 + $i))

  

#   # #remove
#   docker rm -f "$container_name"

#   # 使用 docker run 创建 Redis 容器
#   # docker run -d --name "$container_name" -p "$port:6379" --network docker1 redis

#   # # #restart
#   # docker restart "$container_name"

# done