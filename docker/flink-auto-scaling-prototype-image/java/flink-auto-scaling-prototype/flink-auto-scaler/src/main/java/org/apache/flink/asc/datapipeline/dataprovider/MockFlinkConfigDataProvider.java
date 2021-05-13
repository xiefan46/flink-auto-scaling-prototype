package org.apache.flink.asc.datapipeline.dataprovider;

import com.linkedin.asc.config.Config;
import com.linkedin.asc.config.MapConfig;
import com.linkedin.asc.datapipeline.dataprovider.ConfigDataProvider;
import com.linkedin.asc.model.JobKey;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.asc.config.ConfigConstants;


/**
 * TODO: Talk to the Control plane team and flink hilo team to discuss about how to get Flink configs
 */
public class MockFlinkConfigDataProvider implements ConfigDataProvider {

  @Override
  public Config getJobConfig(JobKey jobKey) {
    Map<String, String> map = new HashMap<>();
    map.put(ConfigConstants.JOB_AUTO_SCALING_CONTAINER_COUNT_KEY, 1 + "");
    map.put(ConfigConstants.JOB_AUTO_SCALING_CONTAINER_MEMORY_MB_KEY, 1024 + "");
    map.put(ConfigConstants.JOB_AUTO_SCALING_CONTAINER_NUM_CORES_PER_CONTAINER_KEY, 4 + "");
    return new MapConfig(map);
  }
}
