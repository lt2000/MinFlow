# recommend not to use the alpine one, it lacks lots of dependencies
# the slim one ocuppies about 2x space compared to alpine one
# FROM python:3.7-alpine
FROM python:3.9-slim

COPY pip.conf /etc/pip.conf
COPY sources.list /etc/apt/sources.list
RUN rm -r  /etc/apt/sources.list.d

# fulfill the structure requirement of proxy
RUN mkdir /proxy


WORKDIR /proxy

# proxy server runs under port 5000
EXPOSE 5000

# for alpine base only
# RUN apk update && \
#     apk add --no-cache --virtual .build-deps gcc musl-dev libffi-dev make && \
#     pip install --no-cache-dir gevent flask && \
#     apk del .build-deps

RUN apt-get clean
RUN apt-get update
RUN apt-get install iproute2 -y
RUN apt-get install gcc -y
RUN pip3 install --no-cache-dir gevent flask couchdb numpy pandas boto3 redis


