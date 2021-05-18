package org.apache.flink.diagnostics.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class JobKey {
  private final String jobName;

  private final String jobId;
}
