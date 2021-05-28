package org.apache.flink.diagnostics.model;

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
public class MetricsHeader extends HashMap<String, String> {

  private static final String JOB_NAME_KEY = "<job_name>";

  private static final String JOB_ID_KEY = "<job_id>";

  public String getJobId() {
    return get(JOB_ID_KEY);
  }

  public String getJobName() {
    return get(JOB_NAME_KEY);
  }

}
