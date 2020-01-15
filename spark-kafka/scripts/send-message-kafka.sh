#!/bin/bash

KAFKA_HOME=/home/diego/bin/kafka_2.12-2.3.0
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic "word_count_topic"
