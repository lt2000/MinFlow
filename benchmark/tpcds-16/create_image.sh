docker rm -f $(docker ps -aq --filter label=workflow)

docker image rm workflow_async_base
docker build --no-cache -t workflow_async_base ~/minflow/src/container

docker image rm tpcds-16_part
docker build --no-cache -t tpcds-16_part ~/minflow/benchmark/tpcds-16/part

docker image rm tpcds-16_join
docker build --no-cache -t tpcds-16_join ~/minflow/benchmark/tpcds-16/join

docker image rm tpcds-16_merge
docker build --no-cache -t tpcds-16_merge ~/minflow/benchmark/tpcds-16/merge


