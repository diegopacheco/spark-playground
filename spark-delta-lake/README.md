### Build

```bash
make build
```

### Run

```bash
run-scaled
```

### Create Delta Lake Table

```bash
./run-spark-sql.sh
```
```sql
CREATE TABLE istari (
  name STRING,
  color STRING
) USING ORC;
```

or run via automation...
```bash
./create-delta-spark-sql-table.sh
```

```
❯ ./create-delta-spark-sql-table.sh
:: loading settings :: url = jar:file:/opt/spark/jars/ivy-2.5.1.jar!/org/apache/ivy/core/settings/ivysettings.xml
Ivy Default Cache set to: /root/.ivy2/cache
The jars for the packages stored in: /root/.ivy2/jars
io.delta#delta-core_2.12 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-5595d8d5-c4d7-437a-90d0-1df8527f2f30;1.0
	confs: [default]
	found io.delta#delta-core_2.12;2.1.0 in central
	found io.delta#delta-storage;2.1.0 in central
	found org.antlr#antlr4-runtime;4.8 in central
	found org.codehaus.jackson#jackson-core-asl;1.9.13 in central
downloading https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.1.0/delta-core_2.12-2.1.0.jar ...
	[SUCCESSFUL ] io.delta#delta-core_2.12;2.1.0!delta-core_2.12.jar (143ms)
downloading https://repo1.maven.org/maven2/io/delta/delta-storage/2.1.0/delta-storage-2.1.0.jar ...
	[SUCCESSFUL ] io.delta#delta-storage;2.1.0!delta-storage.jar (28ms)
downloading https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.8/antlr4-runtime-4.8.jar ...
	[SUCCESSFUL ] org.antlr#antlr4-runtime;4.8!antlr4-runtime.jar (34ms)
downloading https://repo1.maven.org/maven2/org/codehaus/jackson/jackson-core-asl/1.9.13/jackson-core-asl-1.9.13.jar ...
	[SUCCESSFUL ] org.codehaus.jackson#jackson-core-asl;1.9.13!jackson-core-asl.jar (33ms)
:: resolution report :: resolve 1089ms :: artifacts dl 248ms
	:: modules in use:
	io.delta#delta-core_2.12;2.1.0 from central in [default]
	io.delta#delta-storage;2.1.0 from central in [default]
	org.antlr#antlr4-runtime;4.8 from central in [default]
	org.codehaus.jackson#jackson-core-asl;1.9.13 from central in [default]
	---------------------------------------------------------------------
	|                  |            modules            ||   artifacts   |
	|       conf       | number| search|dwnlded|evicted|| number|dwnlded|
	---------------------------------------------------------------------
	|      default     |   4   |   4   |   4   |   0   ||   4   |   4   |
	---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-5595d8d5-c4d7-437a-90d0-1df8527f2f30
	confs: [default]
	4 artifacts copied, 0 already retrieved (3759kB/10ms)
24/12/19 07:02:56 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/12/19 07:03:00 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
24/12/19 07:03:00 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
24/12/19 07:03:04 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
24/12/19 07:03:04 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore UNKNOWN@172.18.0.2
Spark master: spark://spark-master:7077, Application Id: app-20241219070258-0004
Time taken: 1.969 seconds
```

### Run Delta lake spark app

```bash
./run-delta-app.sh
```

```
❯ ./run-delta-app.sh
:: loading settings :: url = jar:file:/opt/spark/jars/ivy-2.5.1.jar!/org/apache/ivy/core/settings/ivysettings.xml
Ivy Default Cache set to: /root/.ivy2/cache
The jars for the packages stored in: /root/.ivy2/jars
io.delta#delta-core_2.12 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-aece7e39-a14a-42ac-8098-e1864931bc1a;1.0
	confs: [default]
	found io.delta#delta-core_2.12;2.1.0 in central
	found io.delta#delta-storage;2.1.0 in central
	found org.antlr#antlr4-runtime;4.8 in central
	found org.codehaus.jackson#jackson-core-asl;1.9.13 in central
:: resolution report :: resolve 249ms :: artifacts dl 13ms
	:: modules in use:
	io.delta#delta-core_2.12;2.1.0 from central in [default]
	io.delta#delta-storage;2.1.0 from central in [default]
	org.antlr#antlr4-runtime;4.8 from central in [default]
	org.codehaus.jackson#jackson-core-asl;1.9.13 from central in [default]
	---------------------------------------------------------------------
	|                  |            modules            ||   artifacts   |
	|       conf       | number| search|dwnlded|evicted|| number|dwnlded|
	---------------------------------------------------------------------
	|      default     |   4   |   0   |   0   |   0   ||   4   |   0   |
	---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-aece7e39-a14a-42ac-8098-e1864931bc1a
	confs: [default]
	0 artifacts copied, 4 already retrieved (0kB/11ms)
24/12/19 07:03:37 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
24/12/19 07:03:38 INFO SparkContext: Running Spark version 3.3.3
24/12/19 07:03:38 INFO ResourceUtils: ==============================================================
24/12/19 07:03:38 INFO ResourceUtils: No custom resources configured for spark.driver.
24/12/19 07:03:38 INFO ResourceUtils: ==============================================================
24/12/19 07:03:38 INFO SparkContext: Submitted application: deltaapp
24/12/19 07:03:38 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
24/12/19 07:03:38 INFO ResourceProfile: Limiting resource is cpu
24/12/19 07:03:38 INFO ResourceProfileManager: Added ResourceProfile id: 0
24/12/19 07:03:38 INFO SecurityManager: Changing view acls to: root
24/12/19 07:03:38 INFO SecurityManager: Changing modify acls to: root
24/12/19 07:03:38 INFO SecurityManager: Changing view acls groups to: 
24/12/19 07:03:38 INFO SecurityManager: Changing modify acls groups to: 
24/12/19 07:03:38 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
24/12/19 07:03:38 INFO Utils: Successfully started service 'sparkDriver' on port 33777.
24/12/19 07:03:38 INFO SparkEnv: Registering MapOutputTracker
24/12/19 07:03:39 INFO SparkEnv: Registering BlockManagerMaster
24/12/19 07:03:39 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
24/12/19 07:03:39 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
24/12/19 07:03:39 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
24/12/19 07:03:39 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-607fcd59-abf7-47f3-901d-bd9c2298754d
24/12/19 07:03:39 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
24/12/19 07:03:39 INFO SparkEnv: Registering OutputCommitCoordinator
24/12/19 07:03:39 INFO Utils: Successfully started service 'SparkUI' on port 4040.
24/12/19 07:03:39 INFO SparkContext: Added JAR file:///root/.ivy2/jars/io.delta_delta-core_2.12-2.1.0.jar at spark://5cce3ecb37ee:33777/jars/io.delta_delta-core_2.12-2.1.0.jar with timestamp 1734591818491
24/12/19 07:03:39 INFO SparkContext: Added JAR file:///root/.ivy2/jars/io.delta_delta-storage-2.1.0.jar at spark://5cce3ecb37ee:33777/jars/io.delta_delta-storage-2.1.0.jar with timestamp 1734591818491
24/12/19 07:03:39 INFO SparkContext: Added JAR file:///root/.ivy2/jars/org.antlr_antlr4-runtime-4.8.jar at spark://5cce3ecb37ee:33777/jars/org.antlr_antlr4-runtime-4.8.jar with timestamp 1734591818491
24/12/19 07:03:39 INFO SparkContext: Added JAR file:///root/.ivy2/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar at spark://5cce3ecb37ee:33777/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar with timestamp 1734591818491
24/12/19 07:03:39 INFO SparkContext: Added file file:///root/.ivy2/jars/io.delta_delta-core_2.12-2.1.0.jar at spark://5cce3ecb37ee:33777/files/io.delta_delta-core_2.12-2.1.0.jar with timestamp 1734591818491
24/12/19 07:03:39 INFO Utils: Copying /root/.ivy2/jars/io.delta_delta-core_2.12-2.1.0.jar to /tmp/spark-c66e27e5-027e-4ba9-999d-f09f06cda5e1/userFiles-74958415-1da5-4459-bfe1-990cfb28216a/io.delta_delta-core_2.12-2.1.0.jar
24/12/19 07:03:39 INFO SparkContext: Added file file:///root/.ivy2/jars/io.delta_delta-storage-2.1.0.jar at spark://5cce3ecb37ee:33777/files/io.delta_delta-storage-2.1.0.jar with timestamp 1734591818491
24/12/19 07:03:39 INFO Utils: Copying /root/.ivy2/jars/io.delta_delta-storage-2.1.0.jar to /tmp/spark-c66e27e5-027e-4ba9-999d-f09f06cda5e1/userFiles-74958415-1da5-4459-bfe1-990cfb28216a/io.delta_delta-storage-2.1.0.jar
24/12/19 07:03:39 INFO SparkContext: Added file file:///root/.ivy2/jars/org.antlr_antlr4-runtime-4.8.jar at spark://5cce3ecb37ee:33777/files/org.antlr_antlr4-runtime-4.8.jar with timestamp 1734591818491
24/12/19 07:03:39 INFO Utils: Copying /root/.ivy2/jars/org.antlr_antlr4-runtime-4.8.jar to /tmp/spark-c66e27e5-027e-4ba9-999d-f09f06cda5e1/userFiles-74958415-1da5-4459-bfe1-990cfb28216a/org.antlr_antlr4-runtime-4.8.jar
24/12/19 07:03:39 INFO SparkContext: Added file file:///root/.ivy2/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar at spark://5cce3ecb37ee:33777/files/org.codehaus.jackson_jackson-core-asl-1.9.13.jar with timestamp 1734591818491
24/12/19 07:03:39 INFO Utils: Copying /root/.ivy2/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar to /tmp/spark-c66e27e5-027e-4ba9-999d-f09f06cda5e1/userFiles-74958415-1da5-4459-bfe1-990cfb28216a/org.codehaus.jackson_jackson-core-asl-1.9.13.jar
24/12/19 07:03:39 INFO StandaloneAppClient$ClientEndpoint: Connecting to master spark://spark-master:7077...
24/12/19 07:03:39 INFO TransportClientFactory: Successfully created connection to spark-master/172.18.0.2:7077 after 26 ms (0 ms spent in bootstraps)
24/12/19 07:03:39 INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20241219070339-0005
24/12/19 07:03:39 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20241219070339-0005/0 on worker-20241219065712-172.18.0.5-46479 (172.18.0.5:46479) with 12 core(s)
24/12/19 07:03:39 INFO StandaloneSchedulerBackend: Granted executor ID app-20241219070339-0005/0 on hostPort 172.18.0.5:46479 with 12 core(s), 1024.0 MiB RAM
24/12/19 07:03:39 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20241219070339-0005/1 on worker-20241219065712-172.18.0.6-46539 (172.18.0.6:46539) with 12 core(s)
24/12/19 07:03:39 INFO StandaloneSchedulerBackend: Granted executor ID app-20241219070339-0005/1 on hostPort 172.18.0.6:46539 with 12 core(s), 1024.0 MiB RAM
24/12/19 07:03:39 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20241219070339-0005/2 on worker-20241219065711-172.18.0.4-38211 (172.18.0.4:38211) with 12 core(s)
24/12/19 07:03:39 INFO StandaloneSchedulerBackend: Granted executor ID app-20241219070339-0005/2 on hostPort 172.18.0.4:38211 with 12 core(s), 1024.0 MiB RAM
24/12/19 07:03:39 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 34741.
24/12/19 07:03:39 INFO NettyBlockTransferService: Server created on 5cce3ecb37ee:34741
24/12/19 07:03:39 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
24/12/19 07:03:39 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 5cce3ecb37ee, 34741, None)
24/12/19 07:03:39 INFO BlockManagerMasterEndpoint: Registering block manager 5cce3ecb37ee:34741 with 434.4 MiB RAM, BlockManagerId(driver, 5cce3ecb37ee, 34741, None)
24/12/19 07:03:39 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 5cce3ecb37ee, 34741, None)
24/12/19 07:03:39 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 5cce3ecb37ee, 34741, None)
24/12/19 07:03:39 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20241219070339-0005/0 is now RUNNING
24/12/19 07:03:39 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20241219070339-0005/1 is now RUNNING
24/12/19 07:03:39 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20241219070339-0005/2 is now RUNNING
24/12/19 07:03:39 INFO SingleEventLogFileWriter: Logging events to file:/opt/spark/spark-events/app-20241219070339-0005.inprogress
24/12/19 07:03:40 INFO StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
24/12/19 07:03:41 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
24/12/19 07:03:41 INFO SharedState: Warehouse path is 'file:/opt/spark/spark-warehouse'.
24/12/19 07:03:43 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.5:59796) with ID 0,  ResourceProfileId 0
24/12/19 07:03:43 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.4:33314) with ID 2,  ResourceProfileId 0
24/12/19 07:03:43 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.6:43624) with ID 1,  ResourceProfileId 0
24/12/19 07:03:44 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.5:41773 with 434.4 MiB RAM, BlockManagerId(0, 172.18.0.5, 41773, None)
24/12/19 07:03:44 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.4:40673 with 434.4 MiB RAM, BlockManagerId(2, 172.18.0.4, 40673, None)
24/12/19 07:03:44 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.6:32891 with 434.4 MiB RAM, BlockManagerId(1, 172.18.0.6, 32891, None)
24/12/19 07:03:45 INFO DelegatingLogStore: LogStore `LogStoreAdapter(io.delta.storage.HDFSLogStore)` is used for scheme `file`
24/12/19 07:03:45 INFO DeltaLog: Creating initial snapshot without metadata, because the directory is empty
24/12/19 07:03:45 INFO InitialSnapshot: [tableId=9586b844-a12e-497b-b4b1-ff72b5550fdc] Created snapshot InitialSnapshot(path=file:/opt/spark/data/istari.delta/_delta_log, version=-1, metadata=Metadata(9672d7c6-82e9-421b-875c-ec6b5ef003a4,null,null,Format(parquet,Map()),null,List(),Map(),Some(1734591825298)), logSegment=LogSegment(file:/opt/spark/data/istari.delta/_delta_log,-1,List(),List(),None,-1), checksumOpt=None)
24/12/19 07:03:45 INFO DeltaLog: No delta log found for the Delta table at file:/opt/spark/data/istari.delta/_delta_log
24/12/19 07:03:45 INFO InitialSnapshot: [tableId=9672d7c6-82e9-421b-875c-ec6b5ef003a4] Created snapshot InitialSnapshot(path=file:/opt/spark/data/istari.delta/_delta_log, version=-1, metadata=Metadata(41d94454-5c20-49ce-a357-5c5f4bedb8e1,null,null,Format(parquet,Map()),null,List(),Map(),Some(1734591825355)), logSegment=LogSegment(file:/opt/spark/data/istari.delta/_delta_log,-1,List(),List(),None,-1), checksumOpt=None)
24/12/19 07:03:45 INFO OptimisticTransaction: [tableId=41d94454,txnId=c941e4e9] Updated metadata from - to Metadata(61e07f26-05b7-44a2-9807-19d9601afe86,null,null,Format(parquet,Map()),{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"color","type":"string","nullable":true,"metadata":{}}]},List(),Map(),Some(1734591825369))
24/12/19 07:03:45 INFO DeltaParquetFileFormat: Using default output committer for Parquet: org.apache.parquet.hadoop.ParquetOutputCommitter
24/12/19 07:03:45 INFO CodeGenerator: Code generated in 130.811528 ms
24/12/19 07:03:45 INFO SparkContext: Starting job: save at NativeMethodAccessorImpl.java:0
24/12/19 07:03:45 INFO DAGScheduler: Got job 0 (save at NativeMethodAccessorImpl.java:0) with 2 output partitions
24/12/19 07:03:45 INFO DAGScheduler: Final stage: ResultStage 0 (save at NativeMethodAccessorImpl.java:0)
24/12/19 07:03:45 INFO DAGScheduler: Parents of final stage: List()
24/12/19 07:03:45 INFO DAGScheduler: Missing parents: List()
24/12/19 07:03:46 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[5] at save at NativeMethodAccessorImpl.java:0), which has no missing parents
24/12/19 07:03:46 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 335.0 KiB, free 434.1 MiB)
24/12/19 07:03:46 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 121.9 KiB, free 434.0 MiB)
24/12/19 07:03:46 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 5cce3ecb37ee:34741 (size: 121.9 KiB, free: 434.3 MiB)
24/12/19 07:03:46 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1509
24/12/19 07:03:46 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 0 (MapPartitionsRDD[5] at save at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1))
24/12/19 07:03:46 INFO TaskSchedulerImpl: Adding task set 0.0 with 2 tasks resource profile 0
24/12/19 07:03:46 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (172.18.0.5, executor 0, partition 0, PROCESS_LOCAL, 4481 bytes) taskResourceAssignments Map()
24/12/19 07:03:46 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1) (172.18.0.6, executor 1, partition 1, PROCESS_LOCAL, 4528 bytes) taskResourceAssignments Map()
24/12/19 07:03:46 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.5:41773 (size: 121.9 KiB, free: 434.3 MiB)
24/12/19 07:03:46 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.6:32891 (size: 121.9 KiB, free: 434.3 MiB)
24/12/19 07:03:50 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 4527 ms on 172.18.0.5 (executor 0) (1/2)
24/12/19 07:03:50 INFO PythonAccumulatorV2: Connected to AccumulatorServer at host: 127.0.0.1 port: 49269
24/12/19 07:03:50 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 4544 ms on 172.18.0.6 (executor 1) (2/2)
24/12/19 07:03:50 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
24/12/19 07:03:50 INFO DAGScheduler: ResultStage 0 (save at NativeMethodAccessorImpl.java:0) finished in 4.730 s
24/12/19 07:03:50 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
24/12/19 07:03:50 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
24/12/19 07:03:50 INFO DAGScheduler: Job 0 finished: save at NativeMethodAccessorImpl.java:0, took 4.773065 s
24/12/19 07:03:50 INFO FileFormatWriter: Start to commit write Job 76dc0d6f-e681-4d76-826b-7c0d1e4e9125.
24/12/19 07:03:50 INFO FileFormatWriter: Write Job 76dc0d6f-e681-4d76-826b-7c0d1e4e9125 committed. Elapsed time: 2 ms.
24/12/19 07:03:50 INFO FileFormatWriter: Finished processing stats for write job 76dc0d6f-e681-4d76-826b-7c0d1e4e9125.
24/12/19 07:03:52 INFO CodeGenerator: Code generated in 244.630095 ms
24/12/19 07:03:52 INFO SparkContext: Starting job: $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77
24/12/19 07:03:52 INFO DAGScheduler: Job 1 finished: $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77, took 0.000293 s
24/12/19 07:03:52 INFO OptimisticTransaction: [tableId=41d94454,txnId=c941e4e9] Attempting to commit version 0 with 5 actions with Serializable isolation level
24/12/19 07:03:52 INFO DeltaLog: Loading version 0.
24/12/19 07:03:52 INFO Snapshot: [tableId=41d94454-5c20-49ce-a357-5c5f4bedb8e1] DELTA: Compute snapshot for version: 0
24/12/19 07:03:52 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 202.2 KiB, free 433.8 MiB)
24/12/19 07:03:52 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 34.9 KiB, free 433.7 MiB)
24/12/19 07:03:52 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 5cce3ecb37ee:34741 (size: 34.9 KiB, free: 434.2 MiB)
24/12/19 07:03:52 INFO SparkContext: Created broadcast 1 from $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77
24/12/19 07:03:52 INFO DeltaLogFileIndex: Created DeltaLogFileIndex(JSON, numFilesInSegment: 1, totalFileSize: 1491)
24/12/19 07:03:53 INFO FileSourceStrategy: Pushed Filters: 
24/12/19 07:03:53 INFO FileSourceStrategy: Post-Scan Filters: 
24/12/19 07:03:53 INFO FileSourceStrategy: Output Data Schema: struct<txn: struct<appId: string, version: bigint, lastUpdated: bigint ... 1 more fields>, add: struct<path: string, partitionValues: map<string,string>, size: bigint, modificationTime: bigint, dataChange: boolean ... 5 more fields>, remove: struct<path: string, deletionTimestamp: bigint, dataChange: boolean, extendedFileMetadata: boolean, partitionValues: map<string,string> ... 5 more fields>, metaData: struct<id: string, name: string, description: string, format: struct<provider: string, options: map<string,string>>, schemaString: string ... 6 more fields>, protocol: struct<minReaderVersion: int, minWriterVersion: int> ... 5 more fields>
24/12/19 07:03:53 INFO CodeGenerator: Code generated in 164.302268 ms
24/12/19 07:03:53 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 202.2 KiB, free 433.5 MiB)
24/12/19 07:03:53 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 5cce3ecb37ee:34741 in memory (size: 121.9 KiB, free: 434.4 MiB)
24/12/19 07:03:53 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 34.9 KiB, free 433.9 MiB)
24/12/19 07:03:53 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 5cce3ecb37ee:34741 (size: 34.9 KiB, free: 434.3 MiB)
24/12/19 07:03:53 INFO SparkContext: Created broadcast 2 from $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77
24/12/19 07:03:53 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 172.18.0.5:41773 in memory (size: 121.9 KiB, free: 434.4 MiB)
24/12/19 07:03:53 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 172.18.0.6:32891 in memory (size: 121.9 KiB, free: 434.4 MiB)
24/12/19 07:03:53 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4194304 bytes, open cost is considered as scanning 4194304 bytes.
24/12/19 07:03:53 INFO DAGScheduler: Registering RDD 13 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) as input to shuffle 0
24/12/19 07:03:53 INFO DAGScheduler: Got map stage job 2 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) with 1 output partitions
24/12/19 07:03:53 INFO DAGScheduler: Final stage: ShuffleMapStage 1 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77)
24/12/19 07:03:53 INFO DAGScheduler: Parents of final stage: List()
24/12/19 07:03:53 INFO DAGScheduler: Missing parents: List()
24/12/19 07:03:53 INFO DAGScheduler: Submitting ShuffleMapStage 1 (MapPartitionsRDD[13] at $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77), which has no missing parents
24/12/19 07:03:53 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 124.3 KiB, free 433.8 MiB)
24/12/19 07:03:53 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 35.2 KiB, free 433.8 MiB)
24/12/19 07:03:53 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 5cce3ecb37ee:34741 (size: 35.2 KiB, free: 434.3 MiB)
24/12/19 07:03:53 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1509
24/12/19 07:03:53 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 1 (MapPartitionsRDD[13] at $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) (first 15 tasks are for partitions Vector(0))
24/12/19 07:03:53 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks resource profile 0
24/12/19 07:03:53 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2) (172.18.0.5, executor 0, partition 0, PROCESS_LOCAL, 4936 bytes) taskResourceAssignments Map()
24/12/19 07:03:53 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 172.18.0.5:41773 (size: 35.2 KiB, free: 434.4 MiB)
24/12/19 07:03:54 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 172.18.0.5:41773 (size: 34.9 KiB, free: 434.3 MiB)
24/12/19 07:03:54 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.18.0.5:41773 (size: 34.9 KiB, free: 434.3 MiB)
24/12/19 07:03:54 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 786 ms on 172.18.0.5 (executor 0) (1/1)
24/12/19 07:03:54 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
24/12/19 07:03:54 INFO DAGScheduler: ShuffleMapStage 1 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) finished in 0.828 s
24/12/19 07:03:54 INFO DAGScheduler: looking for newly runnable stages
24/12/19 07:03:54 INFO DAGScheduler: running: Set()
24/12/19 07:03:54 INFO DAGScheduler: waiting: Set()
24/12/19 07:03:54 INFO DAGScheduler: failed: Set()
24/12/19 07:03:54 INFO CodeGenerator: Generated method too long to be JIT compiled: org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage3.serializefromobject_doConsume_0$ is 14899 bytes
24/12/19 07:03:54 INFO CodeGenerator: Code generated in 134.335949 ms
24/12/19 07:03:54 INFO CodeGenerator: Code generated in 52.048236 ms
24/12/19 07:03:55 INFO CodeGenerator: Code generated in 42.625358 ms
24/12/19 07:03:55 INFO DAGScheduler: Registering RDD 23 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) as input to shuffle 1
24/12/19 07:03:55 INFO DAGScheduler: Got map stage job 3 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) with 50 output partitions
24/12/19 07:03:55 INFO DAGScheduler: Final stage: ShuffleMapStage 3 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77)
24/12/19 07:03:55 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 2)
24/12/19 07:03:55 INFO DAGScheduler: Missing parents: List()
24/12/19 07:03:55 INFO DAGScheduler: Submitting ShuffleMapStage 3 (MapPartitionsRDD[23] at $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77), which has no missing parents
24/12/19 07:03:55 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 423.6 KiB, free 433.4 MiB)
24/12/19 07:03:55 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 101.6 KiB, free 433.3 MiB)
24/12/19 07:03:55 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on 5cce3ecb37ee:34741 (size: 101.6 KiB, free: 434.2 MiB)
24/12/19 07:03:55 INFO SparkContext: Created broadcast 4 from broadcast at DAGScheduler.scala:1509
24/12/19 07:03:55 INFO DAGScheduler: Submitting 50 missing tasks from ShuffleMapStage 3 (MapPartitionsRDD[23] at $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
24/12/19 07:03:55 INFO TaskSchedulerImpl: Adding task set 3.0 with 50 tasks resource profile 0
24/12/19 07:03:55 INFO TaskSetManager: Starting task 5.0 in stage 3.0 (TID 3) (172.18.0.5, executor 0, partition 5, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 30.0 in stage 3.0 (TID 4) (172.18.0.5, executor 0, partition 30, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 42.0 in stage 3.0 (TID 5) (172.18.0.5, executor 0, partition 42, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 0.0 in stage 3.0 (TID 6) (172.18.0.6, executor 1, partition 0, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 1.0 in stage 3.0 (TID 7) (172.18.0.4, executor 2, partition 1, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 2.0 in stage 3.0 (TID 8) (172.18.0.5, executor 0, partition 2, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 3.0 in stage 3.0 (TID 9) (172.18.0.6, executor 1, partition 3, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 4.0 in stage 3.0 (TID 10) (172.18.0.4, executor 2, partition 4, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 6.0 in stage 3.0 (TID 11) (172.18.0.5, executor 0, partition 6, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 7.0 in stage 3.0 (TID 12) (172.18.0.6, executor 1, partition 7, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 8.0 in stage 3.0 (TID 13) (172.18.0.4, executor 2, partition 8, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 9.0 in stage 3.0 (TID 14) (172.18.0.5, executor 0, partition 9, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 10.0 in stage 3.0 (TID 15) (172.18.0.6, executor 1, partition 10, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 11.0 in stage 3.0 (TID 16) (172.18.0.4, executor 2, partition 11, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 12.0 in stage 3.0 (TID 17) (172.18.0.5, executor 0, partition 12, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 13.0 in stage 3.0 (TID 18) (172.18.0.6, executor 1, partition 13, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 14.0 in stage 3.0 (TID 19) (172.18.0.4, executor 2, partition 14, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 15.0 in stage 3.0 (TID 20) (172.18.0.5, executor 0, partition 15, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 16.0 in stage 3.0 (TID 21) (172.18.0.6, executor 1, partition 16, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 17.0 in stage 3.0 (TID 22) (172.18.0.4, executor 2, partition 17, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 18.0 in stage 3.0 (TID 23) (172.18.0.5, executor 0, partition 18, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 19.0 in stage 3.0 (TID 24) (172.18.0.6, executor 1, partition 19, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 20.0 in stage 3.0 (TID 25) (172.18.0.4, executor 2, partition 20, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 21.0 in stage 3.0 (TID 26) (172.18.0.5, executor 0, partition 21, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 22.0 in stage 3.0 (TID 27) (172.18.0.6, executor 1, partition 22, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 23.0 in stage 3.0 (TID 28) (172.18.0.4, executor 2, partition 23, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 24.0 in stage 3.0 (TID 29) (172.18.0.5, executor 0, partition 24, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 25.0 in stage 3.0 (TID 30) (172.18.0.6, executor 1, partition 25, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 26.0 in stage 3.0 (TID 31) (172.18.0.4, executor 2, partition 26, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 27.0 in stage 3.0 (TID 32) (172.18.0.5, executor 0, partition 27, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 28.0 in stage 3.0 (TID 33) (172.18.0.6, executor 1, partition 28, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 29.0 in stage 3.0 (TID 34) (172.18.0.4, executor 2, partition 29, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 31.0 in stage 3.0 (TID 35) (172.18.0.6, executor 1, partition 31, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 32.0 in stage 3.0 (TID 36) (172.18.0.4, executor 2, partition 32, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 33.0 in stage 3.0 (TID 37) (172.18.0.6, executor 1, partition 33, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO TaskSetManager: Starting task 34.0 in stage 3.0 (TID 38) (172.18.0.4, executor 2, partition 34, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:55 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on 172.18.0.5:41773 (size: 101.6 KiB, free: 434.2 MiB)
24/12/19 07:03:55 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on 172.18.0.6:32891 (size: 101.6 KiB, free: 434.3 MiB)
24/12/19 07:03:55 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on 172.18.0.4:40673 (size: 101.6 KiB, free: 434.3 MiB)
24/12/19 07:03:55 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 172.18.0.5:59796
24/12/19 07:03:56 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 172.18.0.6:43624
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_18 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_2 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_6 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_24 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_9 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_30 in memory on 172.18.0.5:41773 (size: 410.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_15 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_12 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_5 in memory on 172.18.0.5:41773 (size: 390.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_21 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_27 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_42 in memory on 172.18.0.5:41773 (size: 435.0 B, free: 434.2 MiB)
24/12/19 07:03:58 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 172.18.0.4:33314
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_25 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_31 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_16 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_7 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_10 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_28 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_33 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_19 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_0 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_13 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_22 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO BlockManagerInfo: Added rdd_20_3 in memory on 172.18.0.6:32891 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:03:58 INFO TaskSetManager: Starting task 35.0 in stage 3.0 (TID 39) (172.18.0.5, executor 0, partition 35, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:58 INFO TaskSetManager: Finished task 27.0 in stage 3.0 (TID 32) in 3476 ms on 172.18.0.5 (executor 0) (1/50)
24/12/19 07:03:58 INFO TaskSetManager: Starting task 36.0 in stage 3.0 (TID 40) (172.18.0.5, executor 0, partition 36, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:58 INFO TaskSetManager: Starting task 37.0 in stage 3.0 (TID 41) (172.18.0.5, executor 0, partition 37, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:58 INFO TaskSetManager: Finished task 2.0 in stage 3.0 (TID 8) in 3501 ms on 172.18.0.5 (executor 0) (2/50)
24/12/19 07:03:58 INFO TaskSetManager: Finished task 12.0 in stage 3.0 (TID 17) in 3499 ms on 172.18.0.5 (executor 0) (3/50)
24/12/19 07:03:58 INFO TaskSetManager: Starting task 38.0 in stage 3.0 (TID 42) (172.18.0.5, executor 0, partition 38, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:58 INFO TaskSetManager: Finished task 15.0 in stage 3.0 (TID 20) in 3507 ms on 172.18.0.5 (executor 0) (4/50)
24/12/19 07:03:58 INFO TaskSetManager: Starting task 39.0 in stage 3.0 (TID 43) (172.18.0.5, executor 0, partition 39, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:58 INFO TaskSetManager: Finished task 9.0 in stage 3.0 (TID 14) in 3522 ms on 172.18.0.5 (executor 0) (5/50)
24/12/19 07:03:58 INFO TaskSetManager: Finished task 24.0 in stage 3.0 (TID 29) in 3519 ms on 172.18.0.5 (executor 0) (6/50)
24/12/19 07:03:58 INFO TaskSetManager: Starting task 40.0 in stage 3.0 (TID 44) (172.18.0.5, executor 0, partition 40, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:58 INFO TaskSetManager: Starting task 41.0 in stage 3.0 (TID 45) (172.18.0.5, executor 0, partition 41, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:58 INFO TaskSetManager: Finished task 30.0 in stage 3.0 (TID 4) in 3549 ms on 172.18.0.5 (executor 0) (7/50)
24/12/19 07:03:58 INFO TaskSetManager: Finished task 42.0 in stage 3.0 (TID 5) in 3553 ms on 172.18.0.5 (executor 0) (8/50)
24/12/19 07:03:58 INFO TaskSetManager: Starting task 43.0 in stage 3.0 (TID 46) (172.18.0.5, executor 0, partition 43, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:58 INFO TaskSetManager: Finished task 18.0 in stage 3.0 (TID 23) in 3555 ms on 172.18.0.5 (executor 0) (9/50)
24/12/19 07:03:58 INFO TaskSetManager: Starting task 44.0 in stage 3.0 (TID 47) (172.18.0.5, executor 0, partition 44, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:58 INFO TaskSetManager: Finished task 6.0 in stage 3.0 (TID 11) in 3572 ms on 172.18.0.5 (executor 0) (10/50)
24/12/19 07:03:58 INFO TaskSetManager: Starting task 45.0 in stage 3.0 (TID 48) (172.18.0.5, executor 0, partition 45, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:58 INFO TaskSetManager: Finished task 5.0 in stage 3.0 (TID 3) in 3588 ms on 172.18.0.5 (executor 0) (11/50)
24/12/19 07:03:58 INFO TaskSetManager: Starting task 46.0 in stage 3.0 (TID 49) (172.18.0.5, executor 0, partition 46, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:58 INFO TaskSetManager: Finished task 21.0 in stage 3.0 (TID 26) in 3585 ms on 172.18.0.5 (executor 0) (12/50)
24/12/19 07:03:58 INFO TaskSetManager: Starting task 47.0 in stage 3.0 (TID 50) (172.18.0.5, executor 0, partition 47, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_40 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_45 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_37 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_39 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_36 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_38 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_44 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO TaskSetManager: Starting task 48.0 in stage 3.0 (TID 51) (172.18.0.5, executor 0, partition 48, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:59 INFO TaskSetManager: Finished task 45.0 in stage 3.0 (TID 48) in 401 ms on 172.18.0.5 (executor 0) (13/50)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_41 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO TaskSetManager: Starting task 49.0 in stage 3.0 (TID 52) (172.18.0.5, executor 0, partition 49, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
24/12/19 07:03:59 INFO TaskSetManager: Finished task 38.0 in stage 3.0 (TID 42) in 592 ms on 172.18.0.5 (executor 0) (14/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 40.0 in stage 3.0 (TID 44) in 561 ms on 172.18.0.5 (executor 0) (15/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 39.0 in stage 3.0 (TID 43) in 612 ms on 172.18.0.5 (executor 0) (16/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 44.0 in stage 3.0 (TID 47) in 594 ms on 172.18.0.5 (executor 0) (17/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 41.0 in stage 3.0 (TID 45) in 631 ms on 172.18.0.5 (executor 0) (18/50)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_47 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 37.0 in stage 3.0 (TID 41) in 731 ms on 172.18.0.5 (executor 0) (19/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 36.0 in stage 3.0 (TID 40) in 741 ms on 172.18.0.5 (executor 0) (20/50)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_35 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_46 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_48 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 48.0 in stage 3.0 (TID 51) in 364 ms on 172.18.0.5 (executor 0) (21/50)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_43 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 47.0 in stage 3.0 (TID 50) in 771 ms on 172.18.0.5 (executor 0) (22/50)
24/12/19 07:03:59 INFO BlockManagerInfo: Added rdd_20_49 in memory on 172.18.0.5:41773 (size: 46.0 B, free: 434.2 MiB)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 46.0 in stage 3.0 (TID 49) in 840 ms on 172.18.0.5 (executor 0) (23/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 43.0 in stage 3.0 (TID 46) in 909 ms on 172.18.0.5 (executor 0) (24/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 35.0 in stage 3.0 (TID 39) in 986 ms on 172.18.0.5 (executor 0) (25/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 49.0 in stage 3.0 (TID 52) in 440 ms on 172.18.0.5 (executor 0) (26/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 22.0 in stage 3.0 (TID 27) in 4631 ms on 172.18.0.6 (executor 1) (27/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 33.0 in stage 3.0 (TID 37) in 4628 ms on 172.18.0.6 (executor 1) (28/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 31.0 in stage 3.0 (TID 35) in 4639 ms on 172.18.0.6 (executor 1) (29/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 13.0 in stage 3.0 (TID 18) in 4648 ms on 172.18.0.6 (executor 1) (30/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 3.0 in stage 3.0 (TID 9) in 4654 ms on 172.18.0.6 (executor 1) (31/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 16.0 in stage 3.0 (TID 21) in 4654 ms on 172.18.0.6 (executor 1) (32/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 25.0 in stage 3.0 (TID 30) in 4654 ms on 172.18.0.6 (executor 1) (33/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 10.0 in stage 3.0 (TID 15) in 4673 ms on 172.18.0.6 (executor 1) (34/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 28.0 in stage 3.0 (TID 33) in 4671 ms on 172.18.0.6 (executor 1) (35/50)
24/12/19 07:03:59 INFO TaskSetManager: Finished task 7.0 in stage 3.0 (TID 12) in 4683 ms on 172.18.0.6 (executor 1) (36/50)
24/12/19 07:04:00 INFO TaskSetManager: Finished task 19.0 in stage 3.0 (TID 24) in 4684 ms on 172.18.0.6 (executor 1) (37/50)
24/12/19 07:04:00 INFO TaskSetManager: Finished task 0.0 in stage 3.0 (TID 6) in 4698 ms on 172.18.0.6 (executor 1) (38/50)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_11 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_32 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_4 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_14 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_26 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_29 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_23 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_8 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_17 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_20 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_1 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added rdd_20_34 in memory on 172.18.0.4:40673 (size: 46.0 B, free: 434.3 MiB)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 20.0 in stage 3.0 (TID 25) in 7343 ms on 172.18.0.4 (executor 2) (39/50)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 11.0 in stage 3.0 (TID 16) in 7348 ms on 172.18.0.4 (executor 2) (40/50)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 1.0 in stage 3.0 (TID 7) in 7351 ms on 172.18.0.4 (executor 2) (41/50)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 34.0 in stage 3.0 (TID 38) in 7340 ms on 172.18.0.4 (executor 2) (42/50)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 32.0 in stage 3.0 (TID 36) in 7342 ms on 172.18.0.4 (executor 2) (43/50)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 26.0 in stage 3.0 (TID 31) in 7344 ms on 172.18.0.4 (executor 2) (44/50)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 4.0 in stage 3.0 (TID 10) in 7354 ms on 172.18.0.4 (executor 2) (45/50)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 14.0 in stage 3.0 (TID 19) in 7351 ms on 172.18.0.4 (executor 2) (46/50)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 8.0 in stage 3.0 (TID 13) in 7354 ms on 172.18.0.4 (executor 2) (47/50)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 29.0 in stage 3.0 (TID 34) in 7345 ms on 172.18.0.4 (executor 2) (48/50)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 23.0 in stage 3.0 (TID 28) in 7349 ms on 172.18.0.4 (executor 2) (49/50)
24/12/19 07:04:02 INFO TaskSetManager: Finished task 17.0 in stage 3.0 (TID 22) in 7352 ms on 172.18.0.4 (executor 2) (50/50)
24/12/19 07:04:02 INFO TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool 
24/12/19 07:04:02 INFO DAGScheduler: ShuffleMapStage 3 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) finished in 7.452 s
24/12/19 07:04:02 INFO DAGScheduler: looking for newly runnable stages
24/12/19 07:04:02 INFO DAGScheduler: running: Set()
24/12/19 07:04:02 INFO DAGScheduler: waiting: Set()
24/12/19 07:04:02 INFO DAGScheduler: failed: Set()
24/12/19 07:04:02 INFO SparkContext: Starting job: $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77
24/12/19 07:04:02 INFO DAGScheduler: Got job 4 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) with 1 output partitions
24/12/19 07:04:02 INFO DAGScheduler: Final stage: ResultStage 6 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77)
24/12/19 07:04:02 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 5)
24/12/19 07:04:02 INFO DAGScheduler: Missing parents: List()
24/12/19 07:04:02 INFO DAGScheduler: Submitting ResultStage 6 (MapPartitionsRDD[26] at $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77), which has no missing parents
24/12/19 07:04:02 INFO MemoryStore: Block broadcast_5 stored as values in memory (estimated size 369.6 KiB, free 432.9 MiB)
24/12/19 07:04:02 INFO MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 90.2 KiB, free 432.8 MiB)
24/12/19 07:04:02 INFO BlockManagerInfo: Added broadcast_5_piece0 in memory on 5cce3ecb37ee:34741 (size: 90.2 KiB, free: 434.1 MiB)
24/12/19 07:04:02 INFO SparkContext: Created broadcast 5 from broadcast at DAGScheduler.scala:1509
24/12/19 07:04:02 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 6 (MapPartitionsRDD[26] at $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) (first 15 tasks are for partitions Vector(0))
24/12/19 07:04:02 INFO TaskSchedulerImpl: Adding task set 6.0 with 1 tasks resource profile 0
24/12/19 07:04:02 INFO TaskSetManager: Starting task 0.0 in stage 6.0 (TID 53) (172.18.0.6, executor 1, partition 0, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:02 INFO BlockManagerInfo: Added broadcast_5_piece0 in memory on 172.18.0.6:32891 (size: 90.2 KiB, free: 434.2 MiB)
24/12/19 07:04:02 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 172.18.0.6:43624
24/12/19 07:04:03 INFO TaskSetManager: Finished task 0.0 in stage 6.0 (TID 53) in 333 ms on 172.18.0.6 (executor 1) (1/1)
24/12/19 07:04:03 INFO TaskSchedulerImpl: Removed TaskSet 6.0, whose tasks have all completed, from pool 
24/12/19 07:04:03 INFO DAGScheduler: ResultStage 6 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) finished in 0.352 s
24/12/19 07:04:03 INFO DAGScheduler: Job 4 is finished. Cancelling potential speculative or zombie tasks for this job
24/12/19 07:04:03 INFO TaskSchedulerImpl: Killing all running tasks in stage 6: Stage finished
24/12/19 07:04:03 INFO DAGScheduler: Job 4 finished: $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77, took 0.358121 s
24/12/19 07:04:03 INFO CodeGenerator: Code generated in 50.460265 ms
24/12/19 07:04:03 INFO Snapshot: [tableId=41d94454-5c20-49ce-a357-5c5f4bedb8e1] DELTA: Done
24/12/19 07:04:03 INFO Snapshot: [tableId=41d94454-5c20-49ce-a357-5c5f4bedb8e1] Created snapshot Snapshot(path=file:/opt/spark/data/istari.delta/_delta_log, version=0, metadata=Metadata(61e07f26-05b7-44a2-9807-19d9601afe86,null,null,Format(parquet,Map()),{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"color","type":"string","nullable":true,"metadata":{}}]},List(),Map(),Some(1734591825369)), logSegment=LogSegment(file:/opt/spark/data/istari.delta/_delta_log,0,ArrayBuffer(SerializableFileStatus(file:/opt/spark/data/istari.delta/_delta_log/00000000000000000000.json,1491,false,1734591832669)),List(),None,1734591832669), checksumOpt=None)
24/12/19 07:04:03 INFO DeltaLog: Updated snapshot to Snapshot(path=file:/opt/spark/data/istari.delta/_delta_log, version=0, metadata=Metadata(61e07f26-05b7-44a2-9807-19d9601afe86,null,null,Format(parquet,Map()),{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"color","type":"string","nullable":true,"metadata":{}}]},List(),Map(),Some(1734591825369)), logSegment=LogSegment(file:/opt/spark/data/istari.delta/_delta_log,0,ArrayBuffer(SerializableFileStatus(file:/opt/spark/data/istari.delta/_delta_log/00000000000000000000.json,1491,false,1734591832669)),List(),None,1734591832669), checksumOpt=None)
24/12/19 07:04:03 INFO OptimisticTransaction: [tableId=41d94454,txnId=c941e4e9] Committed delta #0 to file:/opt/spark/data/istari.delta/_delta_log
Query Result:
24/12/19 07:04:03 INFO PrepareDeltaScan: DELTA: Filtering files for query
24/12/19 07:04:03 INFO SparkContext: Starting job: $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77
24/12/19 07:04:03 INFO DAGScheduler: Got job 5 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) with 50 output partitions
24/12/19 07:04:03 INFO DAGScheduler: Final stage: ResultStage 8 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77)
24/12/19 07:04:03 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 7)
24/12/19 07:04:03 INFO DAGScheduler: Missing parents: List()
24/12/19 07:04:03 INFO DAGScheduler: Submitting ResultStage 8 (MapPartitionsRDD[28] at $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77), which has no missing parents
24/12/19 07:04:03 INFO MemoryStore: Block broadcast_6 stored as values in memory (estimated size 472.8 KiB, free 432.4 MiB)
24/12/19 07:04:03 INFO MemoryStore: Block broadcast_6_piece0 stored as bytes in memory (estimated size 111.7 KiB, free 432.2 MiB)
24/12/19 07:04:03 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on 5cce3ecb37ee:34741 (size: 111.7 KiB, free: 434.0 MiB)
24/12/19 07:04:03 INFO SparkContext: Created broadcast 6 from broadcast at DAGScheduler.scala:1509
24/12/19 07:04:03 INFO DAGScheduler: Submitting 50 missing tasks from ResultStage 8 (MapPartitionsRDD[28] at $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
24/12/19 07:04:03 INFO TaskSchedulerImpl: Adding task set 8.0 with 50 tasks resource profile 0
24/12/19 07:04:03 INFO TaskSetManager: Starting task 0.0 in stage 8.0 (TID 54) (172.18.0.6, executor 1, partition 0, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 1.0 in stage 8.0 (TID 55) (172.18.0.4, executor 2, partition 1, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 2.0 in stage 8.0 (TID 56) (172.18.0.5, executor 0, partition 2, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 3.0 in stage 8.0 (TID 57) (172.18.0.6, executor 1, partition 3, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 4.0 in stage 8.0 (TID 58) (172.18.0.4, executor 2, partition 4, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 5.0 in stage 8.0 (TID 59) (172.18.0.5, executor 0, partition 5, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 7.0 in stage 8.0 (TID 60) (172.18.0.6, executor 1, partition 7, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 8.0 in stage 8.0 (TID 61) (172.18.0.4, executor 2, partition 8, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 6.0 in stage 8.0 (TID 62) (172.18.0.5, executor 0, partition 6, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 10.0 in stage 8.0 (TID 63) (172.18.0.6, executor 1, partition 10, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 11.0 in stage 8.0 (TID 64) (172.18.0.4, executor 2, partition 11, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 9.0 in stage 8.0 (TID 65) (172.18.0.5, executor 0, partition 9, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 13.0 in stage 8.0 (TID 66) (172.18.0.6, executor 1, partition 13, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 14.0 in stage 8.0 (TID 67) (172.18.0.4, executor 2, partition 14, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 12.0 in stage 8.0 (TID 68) (172.18.0.5, executor 0, partition 12, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 16.0 in stage 8.0 (TID 69) (172.18.0.6, executor 1, partition 16, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 17.0 in stage 8.0 (TID 70) (172.18.0.4, executor 2, partition 17, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 15.0 in stage 8.0 (TID 71) (172.18.0.5, executor 0, partition 15, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 19.0 in stage 8.0 (TID 72) (172.18.0.6, executor 1, partition 19, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 20.0 in stage 8.0 (TID 73) (172.18.0.4, executor 2, partition 20, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 18.0 in stage 8.0 (TID 74) (172.18.0.5, executor 0, partition 18, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 22.0 in stage 8.0 (TID 75) (172.18.0.6, executor 1, partition 22, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 23.0 in stage 8.0 (TID 76) (172.18.0.4, executor 2, partition 23, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 21.0 in stage 8.0 (TID 77) (172.18.0.5, executor 0, partition 21, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 25.0 in stage 8.0 (TID 78) (172.18.0.6, executor 1, partition 25, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 26.0 in stage 8.0 (TID 79) (172.18.0.4, executor 2, partition 26, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 24.0 in stage 8.0 (TID 80) (172.18.0.5, executor 0, partition 24, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 28.0 in stage 8.0 (TID 81) (172.18.0.6, executor 1, partition 28, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 29.0 in stage 8.0 (TID 82) (172.18.0.4, executor 2, partition 29, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 27.0 in stage 8.0 (TID 83) (172.18.0.5, executor 0, partition 27, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 31.0 in stage 8.0 (TID 84) (172.18.0.6, executor 1, partition 31, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 32.0 in stage 8.0 (TID 85) (172.18.0.4, executor 2, partition 32, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 30.0 in stage 8.0 (TID 86) (172.18.0.5, executor 0, partition 30, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 33.0 in stage 8.0 (TID 87) (172.18.0.6, executor 1, partition 33, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 34.0 in stage 8.0 (TID 88) (172.18.0.4, executor 2, partition 34, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO TaskSetManager: Starting task 35.0 in stage 8.0 (TID 89) (172.18.0.5, executor 0, partition 35, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:03 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on 172.18.0.5:41773 (size: 111.7 KiB, free: 434.1 MiB)
24/12/19 07:04:03 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on 172.18.0.6:32891 (size: 111.7 KiB, free: 434.1 MiB)
24/12/19 07:04:03 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on 172.18.0.4:40673 (size: 111.7 KiB, free: 434.2 MiB)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 33.0 in stage 8.0 (TID 87) in 649 ms on 172.18.0.6 (executor 1) (1/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 0.0 in stage 8.0 (TID 54) in 668 ms on 172.18.0.6 (executor 1) (2/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 16.0 in stage 8.0 (TID 69) in 667 ms on 172.18.0.6 (executor 1) (3/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 25.0 in stage 8.0 (TID 78) in 665 ms on 172.18.0.6 (executor 1) (4/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 3.0 in stage 8.0 (TID 57) in 681 ms on 172.18.0.6 (executor 1) (5/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 31.0 in stage 8.0 (TID 84) in 676 ms on 172.18.0.6 (executor 1) (6/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 10.0 in stage 8.0 (TID 63) in 685 ms on 172.18.0.6 (executor 1) (7/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 7.0 in stage 8.0 (TID 60) in 688 ms on 172.18.0.6 (executor 1) (8/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 22.0 in stage 8.0 (TID 75) in 685 ms on 172.18.0.6 (executor 1) (9/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 28.0 in stage 8.0 (TID 81) in 686 ms on 172.18.0.6 (executor 1) (10/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 19.0 in stage 8.0 (TID 72) in 690 ms on 172.18.0.6 (executor 1) (11/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 13.0 in stage 8.0 (TID 66) in 692 ms on 172.18.0.6 (executor 1) (12/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 32.0 in stage 8.0 (TID 85) in 873 ms on 172.18.0.4 (executor 2) (13/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 20.0 in stage 8.0 (TID 73) in 903 ms on 172.18.0.4 (executor 2) (14/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 29.0 in stage 8.0 (TID 82) in 907 ms on 172.18.0.4 (executor 2) (15/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 17.0 in stage 8.0 (TID 70) in 913 ms on 172.18.0.4 (executor 2) (16/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 11.0 in stage 8.0 (TID 64) in 918 ms on 172.18.0.4 (executor 2) (17/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 26.0 in stage 8.0 (TID 79) in 921 ms on 172.18.0.4 (executor 2) (18/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 8.0 in stage 8.0 (TID 61) in 928 ms on 172.18.0.4 (executor 2) (19/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 14.0 in stage 8.0 (TID 67) in 931 ms on 172.18.0.4 (executor 2) (20/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 34.0 in stage 8.0 (TID 88) in 933 ms on 172.18.0.4 (executor 2) (21/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 1.0 in stage 8.0 (TID 55) in 946 ms on 172.18.0.4 (executor 2) (22/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 23.0 in stage 8.0 (TID 76) in 941 ms on 172.18.0.4 (executor 2) (23/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 6.0 in stage 8.0 (TID 62) in 950 ms on 172.18.0.5 (executor 0) (24/50)
24/12/19 07:04:04 INFO TaskSetManager: Starting task 36.0 in stage 8.0 (TID 90) (172.18.0.5, executor 0, partition 36, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Finished task 4.0 in stage 8.0 (TID 58) in 955 ms on 172.18.0.4 (executor 2) (25/50)
24/12/19 07:04:04 INFO TaskSetManager: Starting task 37.0 in stage 8.0 (TID 91) (172.18.0.5, executor 0, partition 37, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Finished task 35.0 in stage 8.0 (TID 89) in 958 ms on 172.18.0.5 (executor 0) (26/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 15.0 in stage 8.0 (TID 71) in 965 ms on 172.18.0.5 (executor 0) (27/50)
24/12/19 07:04:04 INFO TaskSetManager: Starting task 38.0 in stage 8.0 (TID 92) (172.18.0.5, executor 0, partition 38, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Finished task 18.0 in stage 8.0 (TID 74) in 970 ms on 172.18.0.5 (executor 0) (28/50)
24/12/19 07:04:04 INFO TaskSetManager: Starting task 39.0 in stage 8.0 (TID 93) (172.18.0.5, executor 0, partition 39, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Starting task 40.0 in stage 8.0 (TID 94) (172.18.0.5, executor 0, partition 40, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Finished task 24.0 in stage 8.0 (TID 80) in 974 ms on 172.18.0.5 (executor 0) (29/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 21.0 in stage 8.0 (TID 77) in 980 ms on 172.18.0.5 (executor 0) (30/50)
24/12/19 07:04:04 INFO TaskSetManager: Starting task 41.0 in stage 8.0 (TID 95) (172.18.0.5, executor 0, partition 41, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Finished task 2.0 in stage 8.0 (TID 56) in 994 ms on 172.18.0.5 (executor 0) (31/50)
24/12/19 07:04:04 INFO TaskSetManager: Starting task 42.0 in stage 8.0 (TID 96) (172.18.0.5, executor 0, partition 42, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Finished task 27.0 in stage 8.0 (TID 83) in 990 ms on 172.18.0.5 (executor 0) (32/50)
24/12/19 07:04:04 INFO TaskSetManager: Starting task 43.0 in stage 8.0 (TID 97) (172.18.0.5, executor 0, partition 43, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Starting task 44.0 in stage 8.0 (TID 98) (172.18.0.5, executor 0, partition 44, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Starting task 45.0 in stage 8.0 (TID 99) (172.18.0.5, executor 0, partition 45, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Finished task 9.0 in stage 8.0 (TID 65) in 1007 ms on 172.18.0.5 (executor 0) (33/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 5.0 in stage 8.0 (TID 59) in 1012 ms on 172.18.0.5 (executor 0) (34/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 30.0 in stage 8.0 (TID 86) in 1005 ms on 172.18.0.5 (executor 0) (35/50)
24/12/19 07:04:04 INFO TaskSetManager: Starting task 46.0 in stage 8.0 (TID 100) (172.18.0.5, executor 0, partition 46, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Finished task 12.0 in stage 8.0 (TID 68) in 1020 ms on 172.18.0.5 (executor 0) (36/50)
24/12/19 07:04:04 INFO TaskSetManager: Starting task 47.0 in stage 8.0 (TID 101) (172.18.0.5, executor 0, partition 47, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Starting task 48.0 in stage 8.0 (TID 102) (172.18.0.5, executor 0, partition 48, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Finished task 42.0 in stage 8.0 (TID 96) in 55 ms on 172.18.0.5 (executor 0) (37/50)
24/12/19 07:04:04 INFO TaskSetManager: Starting task 49.0 in stage 8.0 (TID 103) (172.18.0.5, executor 0, partition 49, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO TaskSetManager: Finished task 36.0 in stage 8.0 (TID 90) in 114 ms on 172.18.0.5 (executor 0) (38/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 38.0 in stage 8.0 (TID 92) in 95 ms on 172.18.0.5 (executor 0) (39/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 40.0 in stage 8.0 (TID 94) in 95 ms on 172.18.0.5 (executor 0) (40/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 37.0 in stage 8.0 (TID 91) in 109 ms on 172.18.0.5 (executor 0) (41/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 39.0 in stage 8.0 (TID 93) in 102 ms on 172.18.0.5 (executor 0) (42/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 44.0 in stage 8.0 (TID 98) in 77 ms on 172.18.0.5 (executor 0) (43/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 46.0 in stage 8.0 (TID 100) in 65 ms on 172.18.0.5 (executor 0) (44/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 47.0 in stage 8.0 (TID 101) in 58 ms on 172.18.0.5 (executor 0) (45/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 45.0 in stage 8.0 (TID 99) in 77 ms on 172.18.0.5 (executor 0) (46/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 43.0 in stage 8.0 (TID 97) in 83 ms on 172.18.0.5 (executor 0) (47/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 41.0 in stage 8.0 (TID 95) in 95 ms on 172.18.0.5 (executor 0) (48/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 49.0 in stage 8.0 (TID 103) in 45 ms on 172.18.0.5 (executor 0) (49/50)
24/12/19 07:04:04 INFO TaskSetManager: Finished task 48.0 in stage 8.0 (TID 102) in 59 ms on 172.18.0.5 (executor 0) (50/50)
24/12/19 07:04:04 INFO TaskSchedulerImpl: Removed TaskSet 8.0, whose tasks have all completed, from pool 
24/12/19 07:04:04 INFO DAGScheduler: ResultStage 8 ($anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77) finished in 1.127 s
24/12/19 07:04:04 INFO DAGScheduler: Job 5 is finished. Cancelling potential speculative or zombie tasks for this job
24/12/19 07:04:04 INFO TaskSchedulerImpl: Killing all running tasks in stage 8: Stage finished
24/12/19 07:04:04 INFO DAGScheduler: Job 5 finished: $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:77, took 1.134874 s
24/12/19 07:04:04 INFO CodeGenerator: Code generated in 22.94741 ms
24/12/19 07:04:04 INFO PrepareDeltaScan: DELTA: Done
24/12/19 07:04:04 INFO FileSourceStrategy: Pushed Filters: 
24/12/19 07:04:04 INFO FileSourceStrategy: Post-Scan Filters: 
24/12/19 07:04:04 INFO FileSourceStrategy: Output Data Schema: struct<name: string, color: string>
24/12/19 07:04:04 INFO CodeGenerator: Code generated in 20.080457 ms
24/12/19 07:04:04 INFO MemoryStore: Block broadcast_7 stored as values in memory (estimated size 203.8 KiB, free 432.0 MiB)
24/12/19 07:04:04 INFO MemoryStore: Block broadcast_7_piece0 stored as bytes in memory (estimated size 35.5 KiB, free 432.0 MiB)
24/12/19 07:04:04 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on 5cce3ecb37ee:34741 (size: 35.5 KiB, free: 434.0 MiB)
24/12/19 07:04:04 INFO SparkContext: Created broadcast 7 from showString at NativeMethodAccessorImpl.java:0
24/12/19 07:04:04 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4194304 bytes, open cost is considered as scanning 4194304 bytes.
24/12/19 07:04:04 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
24/12/19 07:04:04 INFO DAGScheduler: Got job 6 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
24/12/19 07:04:04 INFO DAGScheduler: Final stage: ResultStage 9 (showString at NativeMethodAccessorImpl.java:0)
24/12/19 07:04:04 INFO DAGScheduler: Parents of final stage: List()
24/12/19 07:04:04 INFO DAGScheduler: Missing parents: List()
24/12/19 07:04:04 INFO DAGScheduler: Submitting ResultStage 9 (MapPartitionsRDD[32] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
24/12/19 07:04:04 INFO MemoryStore: Block broadcast_8 stored as values in memory (estimated size 13.5 KiB, free 432.0 MiB)
24/12/19 07:04:04 INFO MemoryStore: Block broadcast_8_piece0 stored as bytes in memory (estimated size 6.1 KiB, free 432.0 MiB)
24/12/19 07:04:04 INFO BlockManagerInfo: Added broadcast_8_piece0 in memory on 5cce3ecb37ee:34741 (size: 6.1 KiB, free: 434.0 MiB)
24/12/19 07:04:04 INFO SparkContext: Created broadcast 8 from broadcast at DAGScheduler.scala:1509
24/12/19 07:04:04 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 9 (MapPartitionsRDD[32] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
24/12/19 07:04:04 INFO TaskSchedulerImpl: Adding task set 9.0 with 1 tasks resource profile 0
24/12/19 07:04:04 INFO TaskSetManager: Starting task 0.0 in stage 9.0 (TID 104) (172.18.0.6, executor 1, partition 0, PROCESS_LOCAL, 4976 bytes) taskResourceAssignments Map()
24/12/19 07:04:04 INFO BlockManagerInfo: Added broadcast_8_piece0 in memory on 172.18.0.6:32891 (size: 6.1 KiB, free: 434.1 MiB)
24/12/19 07:04:05 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on 172.18.0.6:32891 (size: 35.5 KiB, free: 434.1 MiB)
24/12/19 07:04:05 INFO TaskSetManager: Finished task 0.0 in stage 9.0 (TID 104) in 380 ms on 172.18.0.6 (executor 1) (1/1)
24/12/19 07:04:05 INFO TaskSchedulerImpl: Removed TaskSet 9.0, whose tasks have all completed, from pool 
24/12/19 07:04:05 INFO DAGScheduler: ResultStage 9 (showString at NativeMethodAccessorImpl.java:0) finished in 0.408 s
24/12/19 07:04:05 INFO DAGScheduler: Job 6 is finished. Cancelling potential speculative or zombie tasks for this job
24/12/19 07:04:05 INFO TaskSchedulerImpl: Killing all running tasks in stage 9: Stage finished
24/12/19 07:04:05 INFO DAGScheduler: Job 6 finished: showString at NativeMethodAccessorImpl.java:0, took 0.415580 s
24/12/19 07:04:05 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
24/12/19 07:04:05 INFO DAGScheduler: Got job 7 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
24/12/19 07:04:05 INFO DAGScheduler: Final stage: ResultStage 10 (showString at NativeMethodAccessorImpl.java:0)
24/12/19 07:04:05 INFO DAGScheduler: Parents of final stage: List()
24/12/19 07:04:05 INFO DAGScheduler: Missing parents: List()
24/12/19 07:04:05 INFO DAGScheduler: Submitting ResultStage 10 (MapPartitionsRDD[32] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
24/12/19 07:04:05 INFO MemoryStore: Block broadcast_9 stored as values in memory (estimated size 13.5 KiB, free 432.0 MiB)
24/12/19 07:04:05 INFO MemoryStore: Block broadcast_9_piece0 stored as bytes in memory (estimated size 6.1 KiB, free 432.0 MiB)
24/12/19 07:04:05 INFO BlockManagerInfo: Added broadcast_9_piece0 in memory on 5cce3ecb37ee:34741 (size: 6.1 KiB, free: 434.0 MiB)
24/12/19 07:04:05 INFO SparkContext: Created broadcast 9 from broadcast at DAGScheduler.scala:1509
24/12/19 07:04:05 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 10 (MapPartitionsRDD[32] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(1))
24/12/19 07:04:05 INFO TaskSchedulerImpl: Adding task set 10.0 with 1 tasks resource profile 0
24/12/19 07:04:05 INFO TaskSetManager: Starting task 0.0 in stage 10.0 (TID 105) (172.18.0.6, executor 1, partition 1, PROCESS_LOCAL, 4976 bytes) taskResourceAssignments Map()
24/12/19 07:04:05 INFO BlockManagerInfo: Added broadcast_9_piece0 in memory on 172.18.0.6:32891 (size: 6.1 KiB, free: 434.1 MiB)
24/12/19 07:04:05 INFO TaskSetManager: Finished task 0.0 in stage 10.0 (TID 105) in 38 ms on 172.18.0.6 (executor 1) (1/1)
24/12/19 07:04:05 INFO TaskSchedulerImpl: Removed TaskSet 10.0, whose tasks have all completed, from pool 
24/12/19 07:04:05 INFO DAGScheduler: ResultStage 10 (showString at NativeMethodAccessorImpl.java:0) finished in 0.050 s
24/12/19 07:04:05 INFO DAGScheduler: Job 7 is finished. Cancelling potential speculative or zombie tasks for this job
24/12/19 07:04:05 INFO TaskSchedulerImpl: Killing all running tasks in stage 10: Stage finished
24/12/19 07:04:05 INFO DAGScheduler: Job 7 finished: showString at NativeMethodAccessorImpl.java:0, took 0.053556 s
24/12/19 07:04:05 INFO CodeGenerator: Code generated in 8.225992 ms
+--------+-----+
|    name|color|
+--------+-----+
| Gandalf| Grey|
| Saruman|White|
|Radagast|Brown|
+--------+-----+

24/12/19 07:04:05 INFO SparkUI: Stopped Spark web UI at http://5cce3ecb37ee:4040
24/12/19 07:04:05 INFO StandaloneSchedulerBackend: Shutting down all executors
24/12/19 07:04:05 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Asking each executor to shut down
24/12/19 07:04:05 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
24/12/19 07:04:05 INFO MemoryStore: MemoryStore cleared
24/12/19 07:04:05 INFO BlockManager: BlockManager stopped
24/12/19 07:04:05 INFO BlockManagerMaster: BlockManagerMaster stopped
24/12/19 07:04:05 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
24/12/19 07:04:05 WARN Dispatcher: Message RemoteProcessDisconnected(172.18.0.5:59796) dropped. Could not find OutputCommitCoordinator.
24/12/19 07:04:05 INFO SparkContext: Successfully stopped SparkContext
24/12/19 07:04:05 INFO ShutdownHookManager: Shutdown hook called
24/12/19 07:04:05 INFO ShutdownHookManager: Deleting directory /tmp/spark-c66e27e5-027e-4ba9-999d-f09f06cda5e1
24/12/19 07:04:05 INFO ShutdownHookManager: Deleting directory /tmp/spark-c66e27e5-027e-4ba9-999d-f09f06cda5e1/pyspark-9554c087-a852-4b6d-8316-30f07fce1326
24/12/19 07:04:05 INFO ShutdownHookManager: Deleting directory /tmp/spark-00111764-4a3c-448d-b3e0-3a177249a773
```

### Spark cluster URLs

master: http://localhost:9090
history: http://localhost:18080

### Spark SQL

```bash
./run-spark-sql.sh
```

```
❯ ./run-spark-sql.sh
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/12/19 05:59:26 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
24/12/19 05:59:29 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
24/12/19 05:59:29 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
24/12/19 05:59:34 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
24/12/19 05:59:34 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore UNKNOWN@172.19.0.2
24/12/19 05:59:34 WARN ObjectStore: Failed to get database default, returning NoSuchObjectException
Spark master: spark://spark-master:7077, Application Id: app-20241219055927-0001
spark-sql> 
```

### Run pyspark app

```bash
❯ ./run-word-count-python.sh
docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/word_count.py
24/12/19 05:57:11 INFO SparkContext: Running Spark version 3.3.3
24/12/19 05:57:11 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
24/12/19 05:57:11 INFO ResourceUtils: ==============================================================
24/12/19 05:57:11 INFO ResourceUtils: No custom resources configured for spark.driver.
24/12/19 05:57:11 INFO ResourceUtils: ==============================================================
24/12/19 05:57:11 INFO SparkContext: Submitted application: WordCount
24/12/19 05:57:12 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
24/12/19 05:57:12 INFO ResourceProfile: Limiting resource is cpu
24/12/19 05:57:12 INFO ResourceProfileManager: Added ResourceProfile id: 0
24/12/19 05:57:12 INFO SecurityManager: Changing view acls to: root
24/12/19 05:57:12 INFO SecurityManager: Changing modify acls to: root
24/12/19 05:57:12 INFO SecurityManager: Changing view acls groups to: 
24/12/19 05:57:12 INFO SecurityManager: Changing modify acls groups to: 
24/12/19 05:57:12 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
24/12/19 05:57:12 INFO Utils: Successfully started service 'sparkDriver' on port 42391.
24/12/19 05:57:12 INFO SparkEnv: Registering MapOutputTracker
24/12/19 05:57:12 INFO SparkEnv: Registering BlockManagerMaster
24/12/19 05:57:12 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
24/12/19 05:57:12 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
24/12/19 05:57:12 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
24/12/19 05:57:12 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-8501a54c-7aa1-407f-9c8b-ecc3cf4a3da8
24/12/19 05:57:12 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
24/12/19 05:57:12 INFO SparkEnv: Registering OutputCommitCoordinator
24/12/19 05:57:12 INFO Utils: Successfully started service 'SparkUI' on port 4040.
24/12/19 05:57:12 INFO StandaloneAppClient$ClientEndpoint: Connecting to master spark://spark-master:7077...
24/12/19 05:57:12 INFO TransportClientFactory: Successfully created connection to spark-master/172.19.0.2:7077 after 23 ms (0 ms spent in bootstraps)
24/12/19 05:57:12 INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20241219055712-0000
24/12/19 05:57:12 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 38799.
24/12/19 05:57:12 INFO NettyBlockTransferService: Server created on 5aaa409d7deb:38799
24/12/19 05:57:12 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
24/12/19 05:57:12 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 5aaa409d7deb, 38799, None)
24/12/19 05:57:12 INFO BlockManagerMasterEndpoint: Registering block manager 5aaa409d7deb:38799 with 434.4 MiB RAM, BlockManagerId(driver, 5aaa409d7deb, 38799, None)
24/12/19 05:57:12 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20241219055712-0000/0 on worker-20241219055628-172.19.0.6-37871 (172.19.0.6:37871) with 12 core(s)
24/12/19 05:57:12 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 5aaa409d7deb, 38799, None)
24/12/19 05:57:12 INFO StandaloneSchedulerBackend: Granted executor ID app-20241219055712-0000/0 on hostPort 172.19.0.6:37871 with 12 core(s), 1024.0 MiB RAM
24/12/19 05:57:12 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20241219055712-0000/1 on worker-20241219055627-172.19.0.5-43045 (172.19.0.5:43045) with 12 core(s)
24/12/19 05:57:12 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 5aaa409d7deb, 38799, None)
24/12/19 05:57:12 INFO StandaloneSchedulerBackend: Granted executor ID app-20241219055712-0000/1 on hostPort 172.19.0.5:43045 with 12 core(s), 1024.0 MiB RAM
24/12/19 05:57:12 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20241219055712-0000/2 on worker-20241219055627-172.19.0.4-38285 (172.19.0.4:38285) with 12 core(s)
24/12/19 05:57:12 INFO StandaloneSchedulerBackend: Granted executor ID app-20241219055712-0000/2 on hostPort 172.19.0.4:38285 with 12 core(s), 1024.0 MiB RAM
24/12/19 05:57:13 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20241219055712-0000/2 is now RUNNING
24/12/19 05:57:13 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20241219055712-0000/0 is now RUNNING
24/12/19 05:57:13 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20241219055712-0000/1 is now RUNNING
24/12/19 05:57:13 INFO SingleEventLogFileWriter: Logging events to file:/opt/spark/spark-events/app-20241219055712-0000.inprogress
24/12/19 05:57:13 INFO StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
24/12/19 05:57:14 INFO SparkContext: Starting job: collect at /opt/spark/apps/word_count.py:18
24/12/19 05:57:14 INFO DAGScheduler: Registering RDD 2 (reduceByKey at /opt/spark/apps/word_count.py:15) as input to shuffle 0
24/12/19 05:57:14 INFO DAGScheduler: Got job 0 (collect at /opt/spark/apps/word_count.py:18) with 2 output partitions
24/12/19 05:57:14 INFO DAGScheduler: Final stage: ResultStage 1 (collect at /opt/spark/apps/word_count.py:18)
24/12/19 05:57:14 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 0)
24/12/19 05:57:14 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 0)
24/12/19 05:57:14 INFO DAGScheduler: Submitting ShuffleMapStage 0 (PairwiseRDD[2] at reduceByKey at /opt/spark/apps/word_count.py:15), which has no missing parents
24/12/19 05:57:14 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 10.2 KiB, free 434.4 MiB)
24/12/19 05:57:14 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 6.3 KiB, free 434.4 MiB)
24/12/19 05:57:14 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 5aaa409d7deb:38799 (size: 6.3 KiB, free: 434.4 MiB)
24/12/19 05:57:14 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1509
24/12/19 05:57:14 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 0 (PairwiseRDD[2] at reduceByKey at /opt/spark/apps/word_count.py:15) (first 15 tasks are for partitions Vector(0, 1))
24/12/19 05:57:14 INFO TaskSchedulerImpl: Adding task set 0.0 with 2 tasks resource profile 0
24/12/19 05:57:16 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.19.0.4:45884) with ID 2,  ResourceProfileId 0
24/12/19 05:57:16 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.19.0.5:36128) with ID 1,  ResourceProfileId 0
24/12/19 05:57:16 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.19.0.6:43842) with ID 0,  ResourceProfileId 0
24/12/19 05:57:16 INFO BlockManagerMasterEndpoint: Registering block manager 172.19.0.4:45553 with 434.4 MiB RAM, BlockManagerId(2, 172.19.0.4, 45553, None)
24/12/19 05:57:16 INFO BlockManagerMasterEndpoint: Registering block manager 172.19.0.5:43671 with 434.4 MiB RAM, BlockManagerId(1, 172.19.0.5, 43671, None)
24/12/19 05:57:17 INFO BlockManagerMasterEndpoint: Registering block manager 172.19.0.6:46587 with 434.4 MiB RAM, BlockManagerId(0, 172.19.0.6, 46587, None)
24/12/19 05:57:17 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (172.19.0.5, executor 1, partition 0, PROCESS_LOCAL, 4465 bytes) taskResourceAssignments Map()
24/12/19 05:57:17 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1) (172.19.0.5, executor 1, partition 1, PROCESS_LOCAL, 4504 bytes) taskResourceAssignments Map()
24/12/19 05:57:17 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.19.0.5:43671 (size: 6.3 KiB, free: 434.4 MiB)
24/12/19 05:57:18 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 1640 ms on 172.19.0.5 (executor 1) (1/2)
24/12/19 05:57:18 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1676 ms on 172.19.0.5 (executor 1) (2/2)
24/12/19 05:57:18 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
24/12/19 05:57:18 INFO PythonAccumulatorV2: Connected to AccumulatorServer at host: 127.0.0.1 port: 42723
24/12/19 05:57:18 INFO DAGScheduler: ShuffleMapStage 0 (reduceByKey at /opt/spark/apps/word_count.py:15) finished in 4.198 s
24/12/19 05:57:18 INFO DAGScheduler: looking for newly runnable stages
24/12/19 05:57:18 INFO DAGScheduler: running: Set()
24/12/19 05:57:18 INFO DAGScheduler: waiting: Set(ResultStage 1)
24/12/19 05:57:18 INFO DAGScheduler: failed: Set()
24/12/19 05:57:18 INFO DAGScheduler: Submitting ResultStage 1 (PythonRDD[5] at collect at /opt/spark/apps/word_count.py:18), which has no missing parents
24/12/19 05:57:18 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 9.6 KiB, free 434.4 MiB)
24/12/19 05:57:18 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 5.7 KiB, free 434.4 MiB)
24/12/19 05:57:18 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 5aaa409d7deb:38799 (size: 5.7 KiB, free: 434.4 MiB)
24/12/19 05:57:18 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1509
24/12/19 05:57:18 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (PythonRDD[5] at collect at /opt/spark/apps/word_count.py:18) (first 15 tasks are for partitions Vector(0, 1))
24/12/19 05:57:18 INFO TaskSchedulerImpl: Adding task set 1.0 with 2 tasks resource profile 0
24/12/19 05:57:18 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2) (172.19.0.5, executor 1, partition 0, NODE_LOCAL, 4275 bytes) taskResourceAssignments Map()
24/12/19 05:57:18 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3) (172.19.0.5, executor 1, partition 1, PROCESS_LOCAL, 4275 bytes) taskResourceAssignments Map()
24/12/19 05:57:18 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.19.0.5:43671 (size: 5.7 KiB, free: 434.4 MiB)
24/12/19 05:57:18 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 172.19.0.5:36128
24/12/19 05:57:19 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 3) in 324 ms on 172.19.0.5 (executor 1) (1/2)
24/12/19 05:57:19 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 343 ms on 172.19.0.5 (executor 1) (2/2)
24/12/19 05:57:19 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
24/12/19 05:57:19 INFO DAGScheduler: ResultStage 1 (collect at /opt/spark/apps/word_count.py:18) finished in 0.366 s
24/12/19 05:57:19 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
24/12/19 05:57:19 INFO TaskSchedulerImpl: Killing all running tasks in stage 1: Stage finished
24/12/19 05:57:19 INFO DAGScheduler: Job 0 finished: collect at /opt/spark/apps/word_count.py:18, took 4.761280 s
Hello: 3
world: 2
Spark: 1
24/12/19 05:57:19 INFO SparkUI: Stopped Spark web UI at http://5aaa409d7deb:4040
24/12/19 05:57:19 INFO StandaloneSchedulerBackend: Shutting down all executors
24/12/19 05:57:19 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Asking each executor to shut down
24/12/19 05:57:19 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
24/12/19 05:57:19 INFO MemoryStore: MemoryStore cleared
24/12/19 05:57:19 INFO BlockManager: BlockManager stopped
24/12/19 05:57:19 INFO BlockManagerMaster: BlockManagerMaster stopped
24/12/19 05:57:19 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
24/12/19 05:57:19 INFO SparkContext: Successfully stopped SparkContext
24/12/19 05:57:20 INFO ShutdownHookManager: Shutdown hook called
24/12/19 05:57:20 INFO ShutdownHookManager: Deleting directory /tmp/spark-72102e1a-6ec9-4ddd-b630-b394bb1af424/pyspark-1ec8d6af-47f7-4a7e-a0aa-aeca4651312e
24/12/19 05:57:20 INFO ShutdownHookManager: Deleting directory /tmp/spark-72102e1a-6ec9-4ddd-b630-b394bb1af424
24/12/19 05:57:20 INFO ShutdownHookManager: Deleting directory /tmp/spark-c9ddccb9-f5e8-47e5-bca2-3cec99dc48fa
```