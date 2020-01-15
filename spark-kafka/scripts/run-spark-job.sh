#!/bin/bash

SPARK_HOME=~/bin/spark-2.4.4-bin-hadoop2.7/

SPARK_APP="$(pwd)/build/libs/spark-kafka.jar"
echo "Running Spark Job: $SPARK_APP"

$SPARK_HOME/bin/spark-submit \
  --class Main \
  --master local[8] \
  $SPARK_APP \
  "localhost:9092"