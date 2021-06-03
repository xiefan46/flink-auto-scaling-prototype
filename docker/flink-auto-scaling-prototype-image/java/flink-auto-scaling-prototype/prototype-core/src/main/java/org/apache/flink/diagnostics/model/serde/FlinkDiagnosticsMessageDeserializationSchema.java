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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.diagnostics.model.FlinkDiagnosticsMessage;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;


/**
 * A Kafka {@link FlinkDiagnosticsMessageDeserializationSchema} to deserialize {@link FlinkDiagnosticsMessage}s from JSON.
 *
 */
public class FlinkDiagnosticsMessageDeserializationSchema implements DeserializationSchema<FlinkDiagnosticsMessage> {

	private static final long serialVersionUID = 1L;

	private static final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public FlinkDiagnosticsMessage deserialize(byte[] message) throws IOException {
		return objectMapper.readValue(message, FlinkDiagnosticsMessage.class);
	}

	@Override
	public boolean isEndOfStream(FlinkDiagnosticsMessage nextElement) {
		return false;
	}

	@Override
	public TypeInformation<FlinkDiagnosticsMessage> getProducedType() {
		return TypeInformation.of(FlinkDiagnosticsMessage.class);
	}
}
