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

package org.apache.flink.playgrounds.diagnostics.reporter;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.playgrounds.diagnostics.model.DiagnosticsMessage;
import org.apache.flink.playgrounds.diagnostics.model.MetricsHeader;
import org.apache.flink.playgrounds.diagnostics.model.MetricsSnapshot;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.playgrounds.diagnostics.reporter.DiagnosticsMessageReporterOptions.*;


/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via kafka {@link Logger}.
 */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.playgrounds.diagnostics.reporter.DiagnosticsMessageReporterFactory")
public class DiagnosticsMessageReporter implements MetricReporter, CharacterFilter, Scheduled {

  private static final Logger LOG = LoggerFactory.getLogger(DiagnosticsMessageReporter.class);

  private static final char SCOPE_SEPARATOR = '.';
  private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
  private static final CharacterFilter CHARACTER_FILTER = new CharacterFilter() {
    @Override
    public String filterCharacters(String input) {
      return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
    }
  };

  // the initial size roughly fits ~150 metrics with default scope settings
  private KafkaProducer<byte[], byte[]> kafkaProducer;

  private String servers;

  private String topic;

  private String keyBy;

  private String cluster;

  private DiagnosticsMessageSerializationSchema serializationSchema;

  /**
   * Stores all the metrics in a hierarchy way
   * Example:
   *   Root-metric-group
   *     -sub-group1
   *      -metricName1, Metric1
   *     -sub-group2
   *      -metricName2, Metric2
   *      -metricName3, Metric3
   */
  @VisibleForTesting
  Map<String, Object> metricsGroup = new HashMap<>();

  /**
   * Header information about the metrics emitted by this reporter
   */
  private MetricsHeader metricHeader;

  @Override
  public void open(MetricConfig config) {
    cluster = config.getString(CLUSTER.key(), CLUSTER.defaultValue());
    servers = config.getString(SERVERS.key(), SERVERS.defaultValue());
    topic = config.getString(TOPIC.key(), TOPIC.defaultValue());
    keyBy = config.getString(KEY_BY.key(), KEY_BY.defaultValue());
    serializationSchema = new DiagnosticsMessageSerializationSchema(topic);
    if (servers == null) {
      LOG.warn("Cannot find config {}", SERVERS.key());
    }

    Properties properties = new Properties();
    properties.put(SERVERS.key(), servers);
    for (Object keyObj : config.keySet()) {
      String key = keyObj.toString();
      if (key.startsWith("prop.")) {
        properties.put(key.substring(5), config.getString(key, ""));
      }
    }

    //ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      //Thread.currentThread().setContextClassLoader(null);
      kafkaProducer = new KafkaProducer<>(properties);
      LOG.info("Init DiagnosticsStreamMessageReporter successfully. ");
    } catch (Exception e) {
      LOG.warn("DiagnosticsStreamMessageReporter init error.", e);
    } finally {
      //Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }

  @Override
  public void close() {
    if (kafkaProducer != null) {
      kafkaProducer.close();
    }
  }

  @Override
  public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
    synchronized (this) {
      String[] groupNames = getMetricGroupNames(group);
      if (!checkGroupNames(groupNames, metricName)) {
        return;
      }
      Map<String, Object> childGroup = visitMetricGroupMap(groupNames);
      childGroup.put(metricName, metric);
    }
  }

  @Override
  public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
    synchronized (this) {
      String[] groupNames = getMetricGroupNames(group);
      if (!checkGroupNames(groupNames, metricName)) {
        return;
      }
      Map<String, Object> childGroup = visitMetricGroupMap(groupNames);
      childGroup.remove(metricName);
    }
  }

  private boolean checkGroupNames(String[] groupNames, String metricName) {
    if (groupNames == null || groupNames.length == 0) {
      LOG.error("Can not find any group name for metric : {}, skip this metric. ", metricName);
      return false;
    }
    return true;
  }

  /**
   *  Find the child metric group in MetricGroupMap that directly stores this metric
   */
  private Map<String, Object> visitMetricGroupMap(String[] groupNames) {
    Map<String, Object> currentGroup = metricsGroup;
    for (String group : groupNames) {
      Map<String, Object> childGroup = (Map<String, Object>) currentGroup.get(group);
      if (childGroup == null) {
        childGroup = new HashMap<>();
        currentGroup.put(group, childGroup);
      }
      currentGroup = childGroup;
    }
    return currentGroup;
  }

  @Override
  public void report() {
    try {
      synchronized (this) {
        if (kafkaProducer == null) {
          return;
        }
        LOG.info("Report");
        long timestamp = System.currentTimeMillis();
        DiagnosticsMessage diagnosticsMessage = createDiagnosticsMessage();
        ProducerRecord<byte[], byte[]> record = serializationSchema.serialize(diagnosticsMessage, timestamp);
        kafkaProducer.send(record);
      }
    } catch (Exception ignored) {
      LOG.warn("DiagnosticsMessageReporter report error: {}", ignored.getMessage());
    }
  }

  /**
   * We only serialize the metrics we need
   * @return
   */
  @VisibleForTesting
  DiagnosticsMessage createDiagnosticsMessage() {
    MetricsSnapshot metricsSnapshot = MetricsSnapshot.convertToMetricsSnapshot(metricsGroup);
    return new DiagnosticsMessage(metricHeader, metricsSnapshot);
  }

  private String[] getMetricGroupNames(MetricGroup group) {
    String logicalScope =
        ((FrontMetricGroup<AbstractMetricGroup<?>>) group).getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
    if (logicalScope == null || logicalScope.isEmpty()) {
      LOG.error("Can not find any logical scope for group : {}, logical scope : {}", group, logicalScope);
      return null;
    }
    String[] groupNames = logicalScope.split("\\" + SCOPE_SEPARATOR);
    return groupNames;
  }

  @Override
  public String filterCharacters(String input) {
    return input;
  }

  @VisibleForTesting
  void setKafkaProducer(KafkaProducer<byte[], byte[]> kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
  }
}
