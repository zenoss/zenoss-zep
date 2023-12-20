#!/bin/bash

##############################################################################
#
# Copyright (C) Zenoss, Inc. 2023, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################


echo "Stop and remove containers"
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)
echo "Prune docker networks"
docker network prune -f
echo "Creating zep network"
docker network create zep-network
echo "Creating rabbitmq container"
docker run --rm -d \
      --user rabbitmq \
      --name rbtmq-zep \
      --network zep-network \
      --network-alias rabbitmq \
      --add-host rbt0:127.0.0.1 \
      gcr.io/zing-registry-188222/rabbitmq:b9c8d4a \
      /opt/rabbitmq/sbin/rabbitmq-server
echo "Creating redis container"
docker run --rm -d \
    --name redis-zep \
    --network zep-network \
    --network-alias redis \
    gcr.io/zing-registry-188222/redis:83ab373 \
    /usr/bin/redis-server /etc/redis/redis.conf --daemonize no --bind 0.0.0.0
echo "Creating mysql container"
docker run --rm -d \
    --name mysqlzep \
    --network zep-network \
    --network-alias mysql \
    -p 3306:3306 \
    -h 127.0.0.1 \
    -e MYSQL_ROOT_PASSWORD='root' \
    vsamov/mysql-5.1.73:latest
echo "Creating a test RabbitMQ vhost"
sleep 15
docker exec rbtmq-zep rabbitmqctl add_vhost /zenoss-test
docker exec rbtmq-zep rabbitmqctl set_permissions -p /zenoss-test zenoss '.*' '.*' '.*'
echo "Creating mysql-integration-zep-test container"
docker run --rm \
      --name mysql-integration-zep-test \
      --network zep-network \
      -v ${HOME}/.m2:/home/build/.m2 \
      -v $(pwd):/mnt/src \
      -w /mnt/src zenoss/zep-test-image clean verify -Ddbtype=mysql