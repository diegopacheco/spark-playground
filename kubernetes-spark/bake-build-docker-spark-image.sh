#!/bin/bash

eval $(minikube docker-env -p spark)
docker build -t diegopacheco/spark:2.2.1 .