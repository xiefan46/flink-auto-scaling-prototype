/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.reporter;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;


/**
 * Config options for the {@link DiagnosticsMessageReporter}.
 */
@Documentation.SuffixOption
public class DiagnosticsMessageReporterOptions {


	public static final ConfigOption<String> SERVERS = ConfigOptions
		.key("bootstrap.servers")
		.noDefaultValue()
		.withDescription("The kafka bootstrap server host.");

	public static final ConfigOption<String> TOPIC = ConfigOptions
		.key("topic")
		.defaultValue("flink-metrics")
		.withDescription("The metrics topic.");

	public static final ConfigOption<String> KEY_BY = ConfigOptions
		.key("keyBy")
		.defaultValue("")
		.withDescription("The key name of kafka producer");

}
