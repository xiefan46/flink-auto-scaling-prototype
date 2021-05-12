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

import java.util.Collections;
import java.util.Map;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.diagnostics.model.DiagnosticsMessage;
import org.apache.flink.diagnostics.model.serde.DiagnosticsMessageDeserializationSchema;
import org.apache.flink.diagnostics.model.serde.DiagnosticsMessageSerializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.testutils.logging.TestLoggerResource;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.event.Level;


/**
 * Test for {@link DiagnosticsMessageReporter}.
 */
public class DiagnosticsMessageReporterTest {

  private static final String HOST_NAME = "localhost";
  private static final String TASK_MANAGER_ID = "tm01";
  private static final String JOB_NAME = "jn01";
  private static final String TASK_NAME = "tn01";
  private static MetricRegistryImpl registry;
  private static TaskMetricGroup taskMetricGroup;
  private static DiagnosticsMessageReporter reporter;
  private transient double cpuValue = 0;

  @Rule
  public final TestLoggerResource testLoggerResource =
      new TestLoggerResource(DiagnosticsMessageReporter.class, Level.INFO);

  static ReporterSetup createReporterSetup(String reporterName, String servers, String keyBy) {
    MetricConfig metricConfig = new MetricConfig();
    metricConfig.setProperty(DiagnosticsMessageReporterOptions.SERVERS.key(), servers);
    metricConfig.setProperty("keyBy", keyBy);
    metricConfig.setProperty("prop.acks", "all");
    metricConfig.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return ReporterSetup.forReporter(reporterName, metricConfig, new DiagnosticsMessageReporter());
  }

  @BeforeClass
  public static void setUp() {
    Configuration configuration = new Configuration();
    configuration.setString(MetricOptions.SCOPE_NAMING_TASK, "<host>.<tm_id>.<job_name>");

    registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(configuration),
        Collections.singletonList(createReporterSetup("diagnostics-reporter", "localhost:9092", "task_attempt_id")));

    taskMetricGroup =
        new TaskManagerMetricGroup(registry, HOST_NAME, TASK_MANAGER_ID).addTaskForJob(new JobID(), JOB_NAME,
            new JobVertexID(), new ExecutionAttemptID(), TASK_NAME, 0, 0);
    reporter = (DiagnosticsMessageReporter) registry.getReporters().get(0);
  }

  @Test
  public void testReportCounter() throws Exception {
    String counterName = "memory";
    SimpleCounter counter = new SimpleCounter();
    Counter c = taskMetricGroup.counter(counterName, counter);

    DiagnosticsMessage diagnosticsMessage = reporter.createDiagnosticsMessage();
    ObjectMapper om = new ObjectMapper();
    System.out.println(om.writeValueAsString(diagnosticsMessage));
    Map<String, Object> taskGroup = getTaskGroup(diagnosticsMessage.getMetricsSnapshot());
    long c2 = Long.valueOf((String)taskGroup.get(counterName));
    Assert.assertEquals(0L, c2);
    c.inc();
    diagnosticsMessage = reporter.createDiagnosticsMessage();
    taskGroup = getTaskGroup(diagnosticsMessage.getMetricsSnapshot());
    long c3 = Long.valueOf((String)taskGroup.get(counterName));
    Assert.assertEquals(1L, c3);
  }

  @Test
  public void testReportGauge() throws Exception {
    String gaugeName = "cpu";

    Gauge<Double> cpuGauge = new Gauge<Double>() {
      @Override
      public Double getValue() {
        return cpuValue;
      }
    };
    Gauge<Double> g = taskMetricGroup.gauge(gaugeName, cpuGauge);

    DiagnosticsMessage diagnosticsMessage = reporter.createDiagnosticsMessage();
    ObjectMapper om = new ObjectMapper();
    System.out.println(om.writeValueAsString(diagnosticsMessage));
    Map<String, Object> taskGroup = getTaskGroup(diagnosticsMessage.getMetricsSnapshot());
    double g2 = Double.parseDouble((String) taskGroup.get(gaugeName));
    Assert.assertEquals(0d, g2, 0.00001);
    cpuValue += 10.1;
    diagnosticsMessage = reporter.createDiagnosticsMessage();
    taskGroup = getTaskGroup(diagnosticsMessage.getMetricsSnapshot());
    double g3 = Double.parseDouble((String) taskGroup.get(gaugeName));
    Assert.assertEquals(10.1, g3, 0.00001);
  }

  @Test
  public void testSerde() throws Exception {
    long timestamp = System.currentTimeMillis();
    DiagnosticsMessage diagnosticsMessage = reporter.createDiagnosticsMessage();
    DiagnosticsMessageSerializationSchema serializationSchema = new
        DiagnosticsMessageSerializationSchema("t1");
    ProducerRecord<byte[], byte[]> record = serializationSchema.serialize(diagnosticsMessage, timestamp);
    DiagnosticsMessageDeserializationSchema deserializationSchema = new
        DiagnosticsMessageDeserializationSchema();
    DiagnosticsMessage deserialized = deserializationSchema.deserialize(record.value());
    Assert.assertEquals(diagnosticsMessage, deserialized);
  }

  @Test
  public void test() {
    System.out.println(ByteArraySerializer.class.getCanonicalName());
  }

  private Map<String, Object> getTaskGroup(Map<String, Object> metricGroupMap) {
    Map<String, Object> taskManagerGroup = (Map<String, Object>) metricGroupMap.get("taskmanager");
    Assert.assertNotNull(taskManagerGroup);
    Map<String, Object> jobGroup = (Map<String, Object>) taskManagerGroup.get("job");
    Assert.assertNotNull(jobGroup);
    Map<String, Object> taskGroup = (Map<String, Object>) jobGroup.get("task");
    Assert.assertNotNull(taskGroup);
    return taskGroup;
  }

  @AfterClass
  public static void tearDown() throws Exception {
    registry.shutdown().get();
  }
}
