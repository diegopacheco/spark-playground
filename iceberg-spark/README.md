### Tun Spark-SQL with Iceberg

```
❯ ./run-spark-iceberg-sql.sh
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/12/19 05:01:04 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
24/12/19 05:01:05 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
Spark Web UI available at http://1bc7e1d04a40:4041
Spark master: local[*], Application Id: local-1734584465335
spark-sql ()> CREATE TABLE demo.nyc.taxis
            > (
            >   vendor_id bigint,
            >   trip_id bigint,
            >   trip_distance float,
            >   fare_amount double,
            >   store_and_fwd_flag string
            > )
            > PARTITIONED BY (vendor_id);
Time taken: 1.919 seconds
spark-sql ()> INSERT INTO demo.nyc.taxis
            > VALUES (1, 1000371, 1.8, 15.32, 'N'), (2, 1000372, 2.5, 22.15, 'N'), (2, 1000373, 0.9, 9.01, 'N'), (1, 1000374, 8.4, 42.13, 'Y');
Time taken: 3.257 seconds
spark-sql ()> SELECT * FROM demo.nyc.taxis;
1	1000371	1.8	15.32	N
1	1000374	8.4	42.13	Y
2	1000372	2.5	22.15	N
2	1000373	0.9	9.01	N
Time taken: 0.814 seconds, Fetched 4 row(s)
spark-sql ()> 
```