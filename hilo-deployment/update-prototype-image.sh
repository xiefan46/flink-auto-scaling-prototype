#!/bin/bash
cd ../
docker-compose build
cd hilo-deployment
docker image tag apache/flink-auto-scaling-prototype-image-001 xiefan46/flink-auto-scaling-prototype-image:latest
docker image push xiefan46/flink-auto-scaling-prototype-image:latest