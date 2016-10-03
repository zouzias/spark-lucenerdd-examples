#!/bin/bash

DOCKER_NAME="default"
docker-machine start ${DOCKER_NAME}
DOCKER_MACHINE_IP=`docker-machine ip ${DOCKER_NAME}`
eval $(docker-machine env ${DOCKER_NAME})

echo "==========================================="
echo "==========================================="
echo "Browse to http://${DOCKER_MACHINE_IP}:8080/"
echo "==========================================="
echo "==========================================="

docker-compose up


