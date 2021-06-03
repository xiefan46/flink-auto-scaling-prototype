package org.apache.flink.asc.datapipeline.dataprovider;

import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobSize;


public interface ResourceManagerDataProvider {
  boolean verifyCapacity(JobKey jobKey, JobSize currentJobSize, JobSize targetJobSize);
}
