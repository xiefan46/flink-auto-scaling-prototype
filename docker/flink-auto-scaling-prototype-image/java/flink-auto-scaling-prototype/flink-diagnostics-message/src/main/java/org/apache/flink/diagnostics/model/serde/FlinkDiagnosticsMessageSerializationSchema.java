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

package org.apache.flink.diagnostics.model.serde;

import javax.annotation.Nullable;
import org.apache.flink.diagnostics.model.FlinkDiagnosticsMessage;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * A Kafka {@link FlinkDiagnosticsMessageSerializationSchema} to serialize {@link FlinkDiagnosticsMessage}s as JSON.
 *
 */
public class FlinkDiagnosticsMessageSerializationSchema implements KafkaSerializationSchema<FlinkDiagnosticsMessage> {

	private static final ObjectMapper objectMapper = getObjectMapper();

	private String topic;

	public FlinkDiagnosticsMessageSerializationSchema(){
	}

	public FlinkDiagnosticsMessageSerializationSchema(String topic) {
		this.topic = topic;
	}

	private static ObjectMapper getObjectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
		SimpleModule module = new SimpleModule("SamzaModule");
		//module.addKeySerializer(MetricsHeader.class, new MetricsHeaderSerializer());
		//module.addSerializer(MetricHeaderDeserializer.class, new MetricsHeaderSerializer());
		objectMapper.registerModule(module);
		return objectMapper;
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(
			final FlinkDiagnosticsMessage message, @Nullable final Long timestamp) {
		try {
			//if topic is null, default topic will be used
			return new ProducerRecord<>(topic, objectMapper.writeValueAsBytes(message));
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Could not serialize record: " + message, e);
		}
	}




}
