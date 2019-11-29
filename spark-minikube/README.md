## Create proper Minikube profile for Spark
```bash
alias m=minikube
m start -p spark --cpus=4 --memory=4000m
```
## Building Spark Docker image on Kubernetes/Minikube
```bash
eval $(m docker-env -p spark)
CD $SPARK_HOME_INSTALL 
./bin/docker-image-tool.sh -t spark build
``` 
## Prepare Kubernetes Cluster
```bash
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=cluster-admin --serviceaccount=default:spark --namespace=default
```
## Submit Spark Job to the cluster(Run in K8S)
```bash
bin/spark-submit \
    --master k8s://https://192.168.99.101:8443/ \
    --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=3 \
    --conf spark.kubernetes.container.image=spark-py:spark \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar
```   
2.4.4 still not work
https://github.com/kubernetes/kubernetes/issues/82131
