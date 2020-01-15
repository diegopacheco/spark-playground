#!/bin/bash

if [ -z "$1" ]
then
  echo "you must pass the name of the topic."
  exit 1;
fi

KAFKA_HOME=/home/diego/bin/kafka_2.12-2.3.0
$KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $1
echo "topic $1 created in Kafka"
