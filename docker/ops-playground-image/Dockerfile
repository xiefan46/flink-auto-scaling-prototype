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
COPY ./java/flink-playground-clickcountjob /opt/flink-playground-clickcountjob
COPY ./java/diagnostics-message-reporter /opt/diagnostics-message-reporter
WORKDIR /opt/flink-playground-clickcountjob
RUN mvn clean install
WORKDIR /opt/diagnostics-message-reporter
RUN mvn clean install

###############################################################################
# Build Operations Playground Image
###############################################################################

FROM apache/flink:1.12.1-scala_2.11-java8

WORKDIR /opt/flink

# Copy Click Count Job
COPY --from=builder /opt/flink-playground-clickcountjob/target/flink-playground-clickcountjob-*.jar /opt/ClickCountJob.jar
# Copy Diagnostics Metrics Reporter
COPY --from=builder /opt/diagnostics-message-reporter/target/DiagnosticsMessageReporter-*.jar /opt/flink/plugins/DiagnosticsMessageReporter/DiagnosticsMessageReporter.jar
#COPY --from=builder /opt/diagnostics-message-reporter/target/DiagnosticsMessageReporter-*.jar /opt/flink/lib/DiagnosticsMessageReporter.jar