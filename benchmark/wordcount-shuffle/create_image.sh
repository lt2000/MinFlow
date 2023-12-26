docker rm -f $(docker ps -aq --filter label=workflow)

docker image rm wc_count
docker build --no-cache -t wc_count ~/minflow/benchmark/wordcount-shuffle/count

docker image rm wc_merge
docker build --no-cache -t wc_merge ~/minflow/benchmark/wordcount-shuffle/merge

