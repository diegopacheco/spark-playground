#!/bin/bash

#!/bin/bash

docker exec -it da-spark-master spark-sql -e "
CREATE TABLE IF NOT EXISTS istari (
  name STRING,
  color STRING
) USING PARQUET;
"