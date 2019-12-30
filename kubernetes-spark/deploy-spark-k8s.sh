#!/bin/bash

kubectl create -f ./specs/spark-master-deployment.yaml
kubectl create -f ./specs/spark-master-service.yaml
kubectl get deployments
kubectl create -f ./specs/spark-worker-deployment.yaml
kubectl get deployments
kubectl get pods
minikube -p spark addons enable ingress
kubectl apply -f .specs/minikube-ingress.yaml
echo "$(minikube -p spark ip) spark-kubernetes" | sudo tee -a /etc/hosts
kubectl get pods