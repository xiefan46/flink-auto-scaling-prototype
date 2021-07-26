#!/bin/bash

#delete the flink test job
kubectl delete pod click-count-job-client
kubectl delete deployment flink-test-job-taskmanager-deployment
kubectl delete deployment flink-test-job-jobmanager-deployment
kubectl delete deployment clickevent-generator-deployment
kubectl delete service flink-test-job-jobmanager-service

#delete auto-scaler
kubectl delete pod flink-auto-scaler-client
kubectl delete deployment auto-scaler-jobmanager-deployment
kubectl delete deployment flink-auto-scaler-taskmanager-deployment
kubectl delete service auto-scaler-jobmanager-service

#delete kafka and zookeeper
kubectl delete deployment kafka-deployment
kubectl delete service kafka-service
kubectl delete deployment zookeeper-deployment
kubectl delete service zookeeper-service


#delete all the config maps
kubectl delete configmap auto-scaler-config
kubectl delete configmap test-job-config