#!/bin/bash

kubectl create namespace spark
kubectl create -f ./specs/spark-master-deployment.yaml --namespace=spark
kubectl create -f ./specs/spark-master-service.yaml --namespace=spark
kubectl get deployments --namespace=spark
kubectl create -f ./specs/spark-worker-deployment.yaml --namespace=spark
kubectl get deployments --namespace=spark
kubectl get pods --namespace spark
minikube -p spark addons enable ingress
kubectl apply -f ./specs/minikube-ingress.yaml
echo "$(minikube -p spark ip) spark-kubernetes" | sudo tee -a /etc/hosts
kubectl get pods --namespace=spark