#!/bin/bash

kubectl apply -f kafka-all-in-one.yaml
kubectl apply -f clickevent-generator-deployment.yaml
kubectl apply -f click-count-job-deployment.yaml