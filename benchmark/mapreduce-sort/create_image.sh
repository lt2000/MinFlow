docker rm -f $(docker ps -aq --filter label=workflow)

docker image rm minflow_base
docker build --no-cache -t minflow_base ~/minflow/src/base

docker image rm workflow_async_base
docker build --no-cache -t workflow_async_base ~/minflow/src/container

docker image rm sort_part
docker build --no-cache -t sort_part ~/minflow/benchmark/mapreduce-sort/part

docker image rm sort_sort
docker build --no-cache -t sort_sort ~/minflow/benchmark/mapreduce-sort/sort


