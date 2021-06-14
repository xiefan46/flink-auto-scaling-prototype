package com.linkedin.asc.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


/**
 * Encapsulates properties concerning the runtime state of a job.
 */
@ToString(callSuper=true)
@EqualsAndHashCode
public class JobState extends TimestampInfo {

  // Sizing-related parameters of the job
  @Getter
  private final JobSize jobSize;

  private JobState(){
    this(new JobSize(0, 0, 0), -1);
  }

  public JobState(JobSize jobSize, long timestamp) {
    super(timestamp, timestamp);
    this.jobSize = jobSize;
  }

}
