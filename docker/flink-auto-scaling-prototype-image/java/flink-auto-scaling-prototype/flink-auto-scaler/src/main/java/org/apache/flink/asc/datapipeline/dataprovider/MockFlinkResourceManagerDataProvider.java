package org.apache.flink.asc.datapipeline.dataprovider;

import com.linkedin.asc.datapipeline.dataprovider.ResourceManagerDataProvider;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobSize;


public class MockFlinkResourceManagerDataProvider implements ResourceManagerDataProvider {
  @Override
  public boolean verifyCapacity(JobKey jobKey, JobSize currentJobSize, JobSize targetJobSize) {
    return true;
  }
}
