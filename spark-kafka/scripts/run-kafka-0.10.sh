#!/bin/bash

KAFKA_HOME=/home/diego/bin/kafka_2.11-0.10.1.0
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties & 
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > $KAFKA_HOME/kafka.log &
