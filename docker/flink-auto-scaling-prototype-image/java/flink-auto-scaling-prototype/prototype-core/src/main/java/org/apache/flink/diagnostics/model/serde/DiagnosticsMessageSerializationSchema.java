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

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.flink.diagnostics.model.DiagnosticsMessage;
import org.apache.flink.diagnostics.model.MetricsHeader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * A Kafka {@link DiagnosticsMessageSerializationSchema} to serialize {@link DiagnosticsMessage}s as JSON.
 *
 */
public class DiagnosticsMessageSerializationSchema implements KafkaSerializationSchema<DiagnosticsMessage> {

	private static final ObjectMapper objectMapper = getObjectMapper();

	private String topic;

	public DiagnosticsMessageSerializationSchema(){
	}

	public DiagnosticsMessageSerializationSchema(String topic) {
		this.topic = topic;
	}

	private static ObjectMapper getObjectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
		SimpleModule module = new SimpleModule("SamzaModule");
		//module.addKeySerializer(MetricsHeader.class, new MetricsHeaderSerializer());
		//module.addSerializer(MetricsHeader.class, new MetricsHeaderSerializer());
		objectMapper.registerModule(module);
		return objectMapper;
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(
			final DiagnosticsMessage message, @Nullable final Long timestamp) {
		try {
			//if topic is null, default topic will be used
			return new ProducerRecord<>(topic, objectMapper.writeValueAsBytes(message));
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Could not serialize record: " + message, e);
		}
	}

	/*public static class MetricsHeaderSerializer extends JsonSerializer<MetricsHeader> {

		private static final ObjectMapper objectMapper = new ObjectMapper();

		@Override
		public void serialize(MetricsHeader metricsHeader, JsonGenerator jsonGenerator, SerializerProvider provider)
				throws IOException {
			String str = objectMapper.writeValueAsString(metricsHeader);
			jsonGenerator.writeFieldName(str);
		}
	}*/
}
