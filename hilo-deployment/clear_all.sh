#!/bin/bash
kubectl delete deployment clickevent-generator-deployment
kubectl delete deployment click-count-job-deployment
kubectl delete deployment kafka-deployment
kubectl delete service kafka-service
kubectl delete deployment zookeeper-deployment
kubectl delete service zookeeper-service