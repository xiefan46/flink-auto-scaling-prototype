###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

###############################################################################
# Build Click Count Job
###############################################################################

FROM maven:3.6-jdk-8-slim AS builder

# Get Click Count job and compile it
COPY ./java/flink-auto-scaling-prototype /opt/flink-auto-scaling-prototype
WORKDIR /opt/flink-auto-scaling-prototype
RUN mvn clean install -DskipTests


###############################################################################
# Build Operations Playground Image
###############################################################################

FROM apache/flink:1.13.1-scala_2.11-java8

WORKDIR /opt/flink

RUN apt-get update && apt-get install vim -y && apt-get install nmap -y

# Copy Click Count Job
COPY --from=builder /opt/flink-auto-scaling-prototype/flink-playground-clickcountjob/target/flink-playground-clickcountjob-*.jar /opt/ClickCountJob.jar
# Copy Diagnostics Metrics Reporter
COPY --from=builder /opt/flink-auto-scaling-prototype/flink-diagnostics-message-reporter/target/flink-diagnostics-message-reporter-*.jar /opt/flink/plugins/flink-diagnostics-message-reporter/flink-diagnostics-message-reporter.jar
# Copy Flink Auto Scaler
COPY --from=builder /opt/flink-auto-scaling-prototype/flink-auto-scaler/target/flink-auto-scaler-*.jar /opt/FlinkAutoScaler.jar