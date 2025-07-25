#!/bin/bash

set -e

/spark/bin/spark-submit --master spark://spark-master:7077 \
  /app/data_analysis_docker.py --threshold "GoOut,3,false" \
  --app_name spark_app --dataset_file train.csv \
  --threshold "AttendanceRate,0.8,true" --group "Romantic" --group "Race"

  