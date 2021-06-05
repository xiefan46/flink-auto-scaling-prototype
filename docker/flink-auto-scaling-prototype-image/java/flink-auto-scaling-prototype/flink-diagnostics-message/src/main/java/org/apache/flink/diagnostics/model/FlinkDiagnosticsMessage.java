package org.apache.flink.diagnostics.model;

import org.apache.flink.model.DiagnosticsMessage;
import org.apache.flink.model.MetricsSnapshot;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@ToString
@EqualsAndHashCode
public class FlinkDiagnosticsMessage extends DiagnosticsMessage {


  private FlinkDiagnosticsMessage(){
    super(null, null, false, -1);
  }

  public FlinkDiagnosticsMessage(FlinkMetricsHeader metricsHeader, MetricsSnapshot metricsSnapshot, boolean isAutoSizingEnabled, long timestamp) {
    super(metricsHeader, metricsSnapshot, isAutoSizingEnabled, timestamp);
  }

}
