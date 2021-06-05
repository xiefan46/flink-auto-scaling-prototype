package org.apache.flink.diagnostics.model;

import org.apache.flink.model.MetricHeader;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.ToString;


/**
 * TODO:
 *  1. Do we need operator level information
 *  2. Check whether we need to change this for k8s
 */
@ToString
@EqualsAndHashCode
public class FlinkMetricsHeader extends HashMap<String, String> implements MetricHeader {

  private static final String JOB_NAME_KEY = "<job_name>";

  private static final String JOB_ID_KEY = "<job_id>";

  private static final String TM_ID_KEY = "<tm_id>";

  //We assume that job name is the unique identifier of a Flink job
  public String getJobId() {
    return get(JOB_NAME_KEY);
  }

  //We assume that job id indicates two different deployments
  @Override
  public String getJobAttemptId() {
    return get(JOB_ID_KEY);
  }

  //One container only has one TM, so we can use the TM ID to name the container
  @Override
  public String getContainerName() {
    return get(TM_ID_KEY);
  }

  public FlinkMetricsHeader(){
    super();
  }

}
