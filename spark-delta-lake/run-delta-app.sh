#!/bin/bash

docker exec da-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages io.delta:delta-core_2.12:2.1.0 \
  /opt/spark/apps/delta-app.py