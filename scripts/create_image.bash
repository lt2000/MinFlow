docker image rm minflow_base
docker build --no-cache -t minflow_base ~/minflow/src/base

docker image rm workflow_async_base
docker build --no-cache -t workflow_async_base ~/minflow/src/container

# ~/minflow/benchmark/mapreduce-sort/create_image.sh
# ~/minflow/benchmark/tpcds-1/create_image.sh
# ~/minflow/benchmark/tpcds-2/create_image.sh
# ~/minflow/benchmark/wordcount-shuffle/create_image.sh

