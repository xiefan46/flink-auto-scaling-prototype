package org.apache.flink.asc.job;/*
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

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.diagnostics.model.FlinkDiagnosticsMessage;
import org.apache.flink.diagnostics.model.serde.FlinkDiagnosticsMessageDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * This class provides an implementation of the Flink-ASC as a Flink job.
 */
public class FlinkAutoScaler {

  public static final String CHECKPOINTING_OPTION = "checkpointing";
  public static final String EVENT_TIME_OPTION = "event-time";
  public static final String OPERATOR_CHAINING_OPTION = "chaining";

  public static final Time WINDOW_SIZE = Time.of(15, TimeUnit.SECONDS);

  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    configureEnvironment(params, env);

    String inputTopic = params.get("input-topic", "flink-metrics");
    String outputTopic = params.get("output-topic", "output");
    String brokers = params.get("bootstrap.servers", "localhost:9092");
    Properties kafkaProps = new Properties();
    kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "auto-scaler");

    DataStream<FlinkDiagnosticsMessage> source = env.addSource(
        new FlinkKafkaConsumer<FlinkDiagnosticsMessage>(inputTopic, new FlinkDiagnosticsMessageDeserializationSchema(),
            kafkaProps)).name("Flink Metrics Source");

    source.assignTimestampsAndWatermarks(
        WatermarkStrategy.<FlinkDiagnosticsMessage>forBoundedOutOfOrderness(Duration.ofMinutes(10)).withTimestampAssigner(
            (diagnosticsMessage, timestamp) -> diagnosticsMessage.getTimestamp()))
        .keyBy(diagnosticsMessage -> diagnosticsMessage.getMetricsHeader().getJobName())
        .process(new FlinkWindowableTask());

    env.execute("Auto Scaler");
  }

  private static void configureEnvironment(final ParameterTool params, final StreamExecutionEnvironment env) {

    boolean checkpointingEnabled = params.has(CHECKPOINTING_OPTION);
    boolean eventTimeSemantics = params.has(EVENT_TIME_OPTION);
    boolean enableChaining = params.has(OPERATOR_CHAINING_OPTION);

    if (checkpointingEnabled) {
      env.enableCheckpointing(1000);
    }

    if (eventTimeSemantics) {
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    if (!enableChaining) {
      //disabling Operator chaining to make it easier to follow the Job in the WebUI
      env.disableOperatorChaining();
    }
  }
}

