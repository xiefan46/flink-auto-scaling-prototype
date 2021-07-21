#!/bin/bash
#uploads test job and auto-scaler configs to configmap
kubectl create configmap auto-scaler-config --from-file=../conf/auto-scaler-conf/
kubectl create configmap test-job-config --from-file=../conf/test-job-conf/

#deploys all the components of the prototype

#deploys kafka and zookeeper
kubectl apply -f kafka-all-in-one.yaml

#deploys jobmanager and taskmanager for the test job
kubectl apply -f ./test-job-deployment/test-job-jobmanager-deployment-service.yaml
kubectl apply -f ./test-job-deployment/taskmanager-session-deployment.yaml

#deploys a service to generate test data to kafka
kubectl apply -f clickevent-generator-deployment.yaml

#creates a client and submits the click-count-job to jobmanager
#kubectl apply -f click-count-job-deployment.yaml
