docker rm -f $(docker ps -aq --filter label=workflow)

docker image rm sort_part
docker build --no-cache -t sort_part ~/minflow/benchmark/mapreduce-sort/part

docker image rm sort_sort
docker build --no-cache -t sort_sort ~/minflow/benchmark/mapreduce-sort/sort


