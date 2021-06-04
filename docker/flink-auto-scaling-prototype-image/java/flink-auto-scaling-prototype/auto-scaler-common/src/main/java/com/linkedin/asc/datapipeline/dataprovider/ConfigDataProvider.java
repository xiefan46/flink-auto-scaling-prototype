package com.linkedin.asc.datapipeline.dataprovider;

import com.linkedin.asc.config.Config;
import com.linkedin.asc.model.JobKey;


public interface ConfigDataProvider {
  Config getJobConfig(JobKey jobKey);
}
