package org.apache.flink.datapipeline.dataprovider;

import org.apache.flink.model.JobKey;
import org.apache.flink.model.JobSize;


public interface ResourceManagerDataProvider {
  boolean verifyCapacity(JobKey jobKey, JobSize currentJobSize, JobSize targetJobSize);
}
