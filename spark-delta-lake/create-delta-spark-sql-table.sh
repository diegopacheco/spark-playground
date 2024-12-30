#!/bin/bash

docker exec -it da-spark-master spark-sql \
  --packages io.delta:delta-core_2.12:2.1.0 \
  -e "
CREATE TABLE IF NOT EXISTS istari (
  name STRING,
  color STRING
) USING DELTA;
"