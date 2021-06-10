package org.apache.flink.asc.datapipeline.dataprovider;

import com.linkedin.asc.config.Config;
import com.linkedin.asc.datapipeline.dataprovider.ConfigDataProvider;
import com.linkedin.asc.model.JobKey;


/**
 * TODO: Talk to the Control plane team and flink hilo team to discuss about how to get Flink configs
 */
public class MockFlinkConfigDataProvider implements ConfigDataProvider {

  @Override
  public Config getJobConfig(JobKey jobKey) {
    return null;
  }
}
