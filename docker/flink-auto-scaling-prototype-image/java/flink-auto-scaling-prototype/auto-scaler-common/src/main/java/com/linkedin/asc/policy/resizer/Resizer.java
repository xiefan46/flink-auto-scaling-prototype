package com.linkedin.asc.policy.resizer;

import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobSize;
import com.linkedin.asc.model.JobState;


/**
 * For a desired total job heap-memory size, total-memory size (or another resource), the resizer computes the
 * target {@link JobSize} while taking into account constraints on container-size,
 * container-count, etc.
 */
public interface Resizer {

  /**
   * Compute a {@link JobSize} given the current {@link JobState} and a target number of vcores for a job
   * @param job jobKey to identify the job
   * @param currentJobState current state of the job
   * @param newTotalNumVcores target number of vcores for this job.
   * @return
   */
  JobSize resizeToTargetVcores(JobKey job, JobState currentJobState, int newTotalNumVcores);

}
