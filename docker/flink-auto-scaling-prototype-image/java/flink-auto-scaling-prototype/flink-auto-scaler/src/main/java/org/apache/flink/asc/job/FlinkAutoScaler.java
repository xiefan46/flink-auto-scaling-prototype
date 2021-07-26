/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.asc.job;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.diagnostics.model.FlinkDiagnosticsMessage;
import org.apache.flink.diagnostics.model.serde.FlinkDiagnosticsMessageDeserializationSchema;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides an implementation of the Flink-ASC as a Flink job.
 */
public class FlinkAutoScaler {

  private static final String INPUT_TOPIC_NAME = "flink-metrics";

  private static final String KAFKA_BROKERS = "kafka-service:9092";

  private static final String ASC_CONFIG_FILE_PATH = "/opt/flink/conf/flink-asc-config.properties";

  private static final int ASC_PARTITION_COUNT = 1;

  public static final Time WINDOW_SIZE = Time.of(15, TimeUnit.SECONDS);

  private static final Logger LOG = LoggerFactory.getLogger(FlinkAutoScaler.class);

  public static void main(String[] args) throws Exception {
    //We assume that the folder is /opt
    final ParameterTool parameterTool = ParameterTool.fromPropertiesFile(ASC_CONFIG_FILE_PATH);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(parameterTool);
    env.setStateBackend(new MemoryStateBackend(100 * 1024 * 1024, false));

    //debug
    String currentPath = new java.io.File(".").getCanonicalPath();
    System.out.println("Current dir:" + currentPath);
    System.out.println("try get config asc.policy.cpuscaleup.window.ms:" + parameterTool.get("asc.policy.cpuscaleup.window.ms"));

    Properties kafkaProps = new Properties();
    kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "auto-scaler");

    DataStream<FlinkDiagnosticsMessage> source = env.addSource(
        new FlinkKafkaConsumer<FlinkDiagnosticsMessage>(INPUT_TOPIC_NAME,
            new FlinkDiagnosticsMessageDeserializationSchema(), kafkaProps)).name("Flink Metrics Source");



    source.assignTimestampsAndWatermarks(WatermarkStrategy.<FlinkDiagnosticsMessage>forBoundedOutOfOrderness(
        Duration.ofMinutes(10)).withTimestampAssigner(
        (diagnosticsMessage, timestamp) -> diagnosticsMessage.getTimestamp()))
        .filter(diagnosticsMessage -> (diagnosticsMessage.getMetricHeader() != null && diagnosticsMessage.getMetricHeader().getJobId() != null))
        .keyBy(diagnosticsMessage -> diagnosticsMessage.getMetricHeader().getJobId().hashCode() % ASC_PARTITION_COUNT)
        .process(new FlinkWindowableTask());


    env.execute("FlinkAutoScaler");
  }
}

