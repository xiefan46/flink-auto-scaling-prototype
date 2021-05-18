package org.apache.flink.autoscaling.job;/*
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

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.diagnostics.model.DiagnosticsMessage;
import org.apache.flink.diagnostics.model.serde.DiagnosticsMessageDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;


public class FlinkAutoScaler {

	public static final String CHECKPOINTING_OPTION = "checkpointing";
	public static final String EVENT_TIME_OPTION = "event-time";
	public static final String BACKPRESSURE_OPTION = "backpressure";
	public static final String OPERATOR_CHAINING_OPTION = "chaining";

	public static final Time WINDOW_SIZE = Time.of(15, TimeUnit.SECONDS);

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		configureEnvironment(params, env);

		boolean inflictBackpressure = params.has(BACKPRESSURE_OPTION);

		String inputTopic = params.get("input-topic", "input");
		String outputTopic = params.get("output-topic", "output");
		String brokers = params.get("bootstrap.servers", "localhost:9092");
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "click-event-count");

		DataStream<DiagnosticsMessage> clicks =
				env.addSource(new FlinkKafkaConsumer<DiagnosticsMessage>(inputTopic, new DiagnosticsMessageDeserializationSchema(), kafkaProps))
			.name("ClickEvent Source")
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<DiagnosticsMessage>(Time.of(200, TimeUnit.MILLISECONDS)) {
				@Override
				public long extractTimestamp(final DiagnosticsMessage element) {
					return element.getTimestamp();
				}
			});


		/*DataStream<ClickEventStatistics> statistics = clicks
			.keyBy(ClickEvent::getPage)
			.timeWindow(WINDOW_SIZE)
			.aggregate(new CountingAggregator(),
				new ClickEventStatisticsCollector())
			.name("ClickEvent Counter");

		statistics
			.addSink(new FlinkKafkaProducer<>(
				outputTopic,
				new ClickEventStatisticsSerializationSchema(outputTopic),
				kafkaProps,
				FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
			.name("ClickEventStatistics Sink");*/

		env.execute("Click Event Count");
	}

	private static void configureEnvironment(
			final ParameterTool params,
			final StreamExecutionEnvironment env) {

		boolean checkpointingEnabled = params.has(CHECKPOINTING_OPTION);
		boolean eventTimeSemantics = params.has(EVENT_TIME_OPTION);
		boolean enableChaining = params.has(OPERATOR_CHAINING_OPTION);

		if (checkpointingEnabled) {
			env.enableCheckpointing(1000);
		}

		if (eventTimeSemantics) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		}

		if(!enableChaining){
			//disabling Operator chaining to make it easier to follow the Job in the WebUI
			env.disableOperatorChaining();
		}
	}
}
