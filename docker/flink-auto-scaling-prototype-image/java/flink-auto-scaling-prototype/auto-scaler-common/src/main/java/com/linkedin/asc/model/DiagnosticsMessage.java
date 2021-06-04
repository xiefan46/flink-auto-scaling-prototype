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
  protected final MetricHeader metricHeader;

  @Getter
  protected final MetricsSnapshot metricsSnapshot;

  @Getter
  protected final boolean autoScalingEnabled;

  @Getter
  protected final long timestamp;
}
