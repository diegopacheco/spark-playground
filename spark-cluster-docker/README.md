### Build

```bash
make build
```

### Run

```bash
run-scaled
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