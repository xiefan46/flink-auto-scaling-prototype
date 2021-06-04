package org.apache.flink.diagnostics.model;

import com.linkedin.asc.model.DiagnosticsMessage;
import com.linkedin.asc.model.MetricHeader;
import com.linkedin.asc.model.MetricsSnapshot;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


@ToString
@EqualsAndHashCode
public class FlinkDiagnosticsMessage extends DiagnosticsMessage {


  private FlinkDiagnosticsMessage(){
    super(null, null, -1);
  }

  public FlinkDiagnosticsMessage(FlinkMetricsHeader metricsHeader, MetricsSnapshot metricsSnapshot, long timestamp) {
    super(metricsHeader, metricsSnapshot, timestamp);
  }

}
