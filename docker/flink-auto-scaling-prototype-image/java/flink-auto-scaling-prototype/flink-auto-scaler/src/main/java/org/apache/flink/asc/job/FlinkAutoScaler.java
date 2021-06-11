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

import com.linkedin.asc.config.MapConfig;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.asc.config.FlinkASCConfig;
import org.apache.flink.diagnostics.model.FlinkDiagnosticsMessage;
import org.apache.flink.diagnostics.model.serde.FlinkDiagnosticsMessageDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
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

  private static final String KAFKA_BROKERS = "kafka:9092";

  private static final String ASC_CONFIG_FILE_PATH = "./flink/conf/flink-asc-config.properties";

  public static final Time WINDOW_SIZE = Time.of(15, TimeUnit.SECONDS);

  private static final Logger LOG = LoggerFactory.getLogger(FlinkAutoScaler.class);

  public static void main(String[] args) throws Exception {
    //We assume that the folder is /opt
    final ParameterTool flinkASCConfigs = ParameterTool.fromPropertiesFile(ASC_CONFIG_FILE_PATH);

    FlinkASCConfig flinkASCConfig = new FlinkASCConfig(new MapConfig(flinkASCConfigs.toMap()));

    LOG.info("Load all flink asc configs: {}", flinkASCConfig);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


    Properties kafkaProps = new Properties();
    kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "auto-scaler");

    DataStream<FlinkDiagnosticsMessage> source = env.addSource(
        new FlinkKafkaConsumer<FlinkDiagnosticsMessage>(INPUT_TOPIC_NAME, new FlinkDiagnosticsMessageDeserializationSchema(),
            kafkaProps)).name("Flink Metrics Source");

    source.assignTimestampsAndWatermarks(
        WatermarkStrategy.<FlinkDiagnosticsMessage>forBoundedOutOfOrderness(Duration.ofMinutes(10)).withTimestampAssigner(
            (diagnosticsMessage, timestamp) -> diagnosticsMessage.getTimestamp()))
        .keyBy(diagnosticsMessage -> diagnosticsMessage.getMetricHeader().getJobId())
        .process(new FlinkWindowableTask(flinkASCConfig));

    env.execute("FlinkAutoScaler");
  }

}

