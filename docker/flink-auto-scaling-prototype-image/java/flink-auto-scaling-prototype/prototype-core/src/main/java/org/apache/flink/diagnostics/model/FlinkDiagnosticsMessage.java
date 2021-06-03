package org.apache.flink.diagnostics.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


@ToString
@EqualsAndHashCode
public class FlinkDiagnosticsMessage {

  @Getter
  private final MetricsHeader metricsHeader;

  @Getter
  private final MetricsSnapshot metricsSnapshot;

  @Getter
  private long timestamp;

  private FlinkDiagnosticsMessage(){
    metricsHeader = null;
    metricsSnapshot = null;
    timestamp = -1;
  }

  public FlinkDiagnosticsMessage(MetricsHeader metricsHeader, MetricsSnapshot metricsSnapshot, long timestamp) {
    this.metricsHeader = metricsHeader;
    this.metricsSnapshot = metricsSnapshot;
    this.timestamp = timestamp;
  }





}
