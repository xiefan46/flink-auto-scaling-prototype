package com.linkedin.asc.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class DiagnosticsMessage {

  @Getter
  private final MetricHeader metricHeader;

  @Getter
  private final MetricsSnapshot metricsSnapshot;

  @Getter
  private long timestamp;
}
