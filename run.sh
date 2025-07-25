#!/bin/bash

set -e
docker exec -ti namenode sh -c 'hdfs dfs -mkdir /input'
docker exec -ti namenode sh -c 'hdfs dfs -put /data/train.csv /input'
docker run --net bdproj1 --name sparkproj1 sparkproj1:1

