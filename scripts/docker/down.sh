#!/bin/bash

set -x
export COMPOSE_HTTP_TIMEOUT=180

docker-compose -p test-poc down
docker container rm --force $(docker ps --filter "network=test-poc_default" -a -q)
docker network rm test-poc_default
