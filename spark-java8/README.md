## Run with gradle
```bash
./gradlew run 
```
## Submit to run in Spark Cluster
```bash
./gradlew build
~/bin/spark-2.4.4-bin-hadoop2.7/bin/spark-submit --class "SparkMain" build/libs/spark-java8.jar
```
