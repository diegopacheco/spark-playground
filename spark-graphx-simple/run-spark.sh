#!/bin/bash

SPARK_HOME=/home/diego/bin/spark-2.0.2-bin-hadoop2.7/
MAIN_CLASS="com.github.diegopacheco.spark.simple.SimpleSparkGraphXApp"
JAR_NAME="spark-graphx-simple_2.11-1.0.jar"

$SPARK_HOME/bin/spark-submit \
  --class $MAIN_CLASS \
  --master local[4] \
  target/scala-2.11/$JAR_NAME

