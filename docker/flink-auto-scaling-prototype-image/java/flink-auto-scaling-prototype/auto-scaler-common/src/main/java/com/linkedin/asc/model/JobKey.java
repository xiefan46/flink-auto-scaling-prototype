package com.linkedin.asc.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


/**
 * {@link JobKey} represents a unique deployment of a job
 */
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class JobKey {

  /**
   * The unique identifier of a job
   */
  @Getter
  private final String jobId;

  /**
   * The unique identifier of one deployment of a job
   */
  @Getter
  private final String attemptId;

}
