#!/bin/bash
docker restart couchdb
docker rm -f redis
docker run -itd -p 8002:6379 --name redis  --network docker1 redis
