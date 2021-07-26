#!/bin/bash
#uploads test job and auto-scaler configs to configmap
kubectl create configmap auto-scaler-config --from-file=../conf/auto-scaler-conf/ --namespace=samza-testing
kubectl create configmap test-job-config --from-file=../conf/test-job-conf/ --namespace=samza-testing

#deploys all the components of the prototype

#deploys kafka and zookeeper
kubectl apply -f kafka-all-in-one.yaml --namespace=samza-testing

#deploys jobmanager and taskmanager for the test job
kubectl apply -f ./test-job-deployment/test-job-jobmanager-deployment-service.yaml --namespace=samza-testing
kubectl apply -f ./test-job-deployment/taskmanager-session-deployment.yaml --namespace=samza-testing

#deploys a service to generate test data to kafka
kubectl apply -f ./test-job-deployment/clickevent-generator-deployment.yaml --namespace=samza-testing

#creates a client and submits the click-count-job to jobmanager
kubectl apply -f ./test-job-deployment/click-count-job-client.yaml --namespace=samza-testing

#deploys the auto-scaler
kubectl apply -f ./auto-scaler-deployment/auto-scaler-jobmanager-deployment-service.yaml --namespace=samza-testing
kubectl apply -f ./auto-scaler-deployment/taskmanager-session-deployment.yaml --namespace=samza-testing
kubectl apply -f ./auto-scaler-deployment/flink-auto-scaler-client.yaml --namespace=samza-testing

#kubectl port-forward deployment/flink-test-job-jobmanager-deployment 8081:8081
#kubectl port-forward deployment/auto-scaler-jobmanager-deployment 8081:8081

