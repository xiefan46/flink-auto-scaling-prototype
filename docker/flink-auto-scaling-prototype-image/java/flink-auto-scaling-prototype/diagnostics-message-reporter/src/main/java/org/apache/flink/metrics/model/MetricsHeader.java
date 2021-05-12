package org.apache.flink.metrics.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;


/**
 * TODO:
 *  1. Is the job model in flink the same as samza?
 *  2. Do we need operator level information
 *  3. Check whether we need to change this for k8s
 */
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class MetricsHeader {
  private final String jobName;
  private final String jobId;
  private final String containerName;
  private final String host;
  private final String timestamp;

  private MetricsHeader() {
    jobName = "";
    jobId = "";
    containerName = "";
    host = "";
    timestamp = "";
  }
}
