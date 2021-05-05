package org.apache.flink.metrics.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


@ToString
@EqualsAndHashCode
public class DiagnosticsMessage {

  @Getter
  private final MetricsHeader metricsHeader;

  @Getter
  private final MetricsSnapshot metricsSnapshot;

  private DiagnosticsMessage(){
    metricsHeader = null;
    metricsSnapshot = null;
  }

  public DiagnosticsMessage(MetricsHeader metricsHeader, MetricsSnapshot metricsSnapshot) {
    this.metricsHeader = metricsHeader;
    this.metricsSnapshot = metricsSnapshot;
  }





}
