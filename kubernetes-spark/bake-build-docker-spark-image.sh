#!/bin/bash

eval $(minikube docker-env)
docker build -t diegopacheco/spark:2.4.4 .