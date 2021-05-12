package org.apache.flink.diagnostics.model;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricsSnapshot extends HashMap<String, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsSnapshot.class);

  public static MetricsSnapshot convertToMetricsSnapshot(Map<String, Object> metricsGroup) {
    MetricsSnapshot metricsSnapshot = new MetricsSnapshot();
    visit(metricsGroup, metricsSnapshot);
    return metricsSnapshot;
  }

  /**
   * Currently, We only need counter and gauge type in auto scaling
   */
  private static void visit(Map<String, Object> sourceMetricGroup, Map<String, Object> targetMetricGroup) {
    for(Map.Entry<String, Object> entry : sourceMetricGroup.entrySet()) {
      String key = entry.getKey();
      Object o = entry.getValue();
      if (o instanceof Counter) {
        targetMetricGroup.put(entry.getKey(), ((Counter) o).getCount() + "");
      } else if(o instanceof Gauge) {
        targetMetricGroup.put(entry.getKey(), ((Gauge) o).getValue() + "");
      } else if(o instanceof Map) {
        if(!targetMetricGroup.containsKey(key)) {
          targetMetricGroup.put(key, new HashMap<>());
        }
        visit((Map<String, Object>)o, (Map<String, Object>) targetMetricGroup.get(key));
      }
    }
  }
}
