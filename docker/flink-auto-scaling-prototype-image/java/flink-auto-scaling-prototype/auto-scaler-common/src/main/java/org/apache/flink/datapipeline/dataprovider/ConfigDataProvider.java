package org.apache.flink.datapipeline.dataprovider;

import org.apache.flink.config.Config;
import org.apache.flink.model.JobKey;


public interface ConfigDataProvider {
  Config getJobConfig(JobKey jobKey);
}
