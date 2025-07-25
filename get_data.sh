#!/bin/bash

set -e
docker exec -ti namenode sh -c 'hdfs dfs -get /output/threshold /data/threshold'
docker exec -ti namenode sh -c 'hdfs dfs -get /output/group /data/group'
docker exec -ti namenode sh -c 'hdfs dfs -get /output/GPA_by_GoOut /data/GPA_by_GoOut'


