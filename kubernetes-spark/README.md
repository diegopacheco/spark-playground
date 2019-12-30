## Start minikube spark profile
```bash
minikube start -p spark --memory 8192 --cpus 4
```
## Bake Docker Spark Image and Deploy in K8s
```bash
./bake-build-docker-spark-image.sh
./deploy-spark-k8s.sh
```
## Run Spark
```bash
master=$(kubectl get pods --namespace=spark -l 'component=spark-master' -o jsonpath='{.items[*].metadata.name}')
echo "words = 'the quick brown fox jumps over the\
        lazy dog the quick brown fox jumps over the lazy dog'
sc = SparkContext()
seq = words.split()
data = sc.parallelize(seq)
counts = data.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).collect()
dict(counts)
sc.stop()"
kubectl exec $master -it pyspark --namespace=spark
```
## Check the WebUI for Spark
```bash
master=$(kubectl get pods --namespace=spark -l 'component=spark-master' -o jsonpath='{.items[*].metadata.name}')
kubectl port-forward $master 8080:8080 --namespace=spark
xdg-open "http://localhost:8080"
```
