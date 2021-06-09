package org.apache.flink.diagnostics.model;

import com.linkedin.asc.model.MetricsSnapshot;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlinkMetricsSnapshot extends MetricsSnapshot {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkMetricsSnapshot.class);

  public static FlinkMetricsSnapshot convertToMetricsSnapshot(Map<String, Object> metricsGroup) {
    FlinkMetricsSnapshot metricsSnapshot = new FlinkMetricsSnapshot();
    visit(metricsGroup, metricsSnapshot);
    return metricsSnapshot;
  }

  private  FlinkMetricsSnapshot(){

  }



  /**
   * Currently, We only need counter and gauge type in auto scaling
   */
  private static void visit(Map<String, Object> sourceMetricGroup, Map<String, Object> targetMetricGroup) {
    for(Entry<String, Object> entry : sourceMetricGroup.entrySet()) {
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
      } else {
        LOG.debug("Ignore other types");
      }
    }
  }

  /**
   *  Find the child metric group in MetricGroupMap that directly stores this metric
   */
  public static Map<String, Object> visitMetricGroupMap(Map<String, Object> currentGroup, String[] groupNames,
      boolean createChildGroup) {
    for (String group : groupNames) {
      Map<String, Object> childGroup = (Map<String, Object>) currentGroup.get(group);
      if (childGroup == null) {
        if(createChildGroup) {
          childGroup = new HashMap<>();
          currentGroup.put(group, childGroup);
        }else{
          return null;
        }
      }
      currentGroup = childGroup;
    }
    return currentGroup;
  }

}
