#!/bin/bash

#delete the flink test job
kubectl delete deployment click-count-job-deployment
kubectl delete deployment flink-test-job-taskmanager-deployment
kubectl delete deployment flink-test-job-jobmanager-deployment
kubectl delete service flink-test-job-jobmanager-service
kubectl delete deployment click-count-job-deployment
kubectl delete deployment clickevent-generator-deployment

#delete kafka and zookeeper
kubectl delete deployment kafka-deployment
kubectl delete service kafka-service
kubectl delete deployment zookeeper-deployment
kubectl delete service zookeeper-service


#delete all the config maps
kubectl delete configmap auto-scaler-config
kubectl delete configmap test-job-config