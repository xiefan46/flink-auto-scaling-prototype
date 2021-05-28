package org.apache.flink.asc.policy.resizer;

import java.util.Optional;
import org.apache.flink.asc.model.JobKey;
import org.apache.flink.asc.model.JobSize;
import org.apache.flink.asc.model.JobState;


/**
 * For a desired total job heap-memory size, total-memory size (or another resource), the resizer computes the
 * target {@link JobSize} while taking into account constraints on container-size,
 * container-count, etc.
 */
public interface Resizer {
  /**
   * Compute a {@link JobSize}, given the current {@link org.apache.flink.asc.model.JobState} and a target heap memory size (in MB) for the entire job.
   * @param job jobKey to identify the job
   * @param currentJobState current state of the job
   * @param targetJobHeapMemoryMb target heap memory (in MB) of the job
   * @param containerMemoryMbUnit The unit (in MB) to use if rounding-up of containerMemoryMb is desired, empty
   * if rounding-up is not desired.
   * @param assignRoundedUpDiffToHeap If true then assign rounded-up diff to heap, false otherwise
   * @return computed job size
   */
  JobSize resizeToTargetHeapSize(JobKey job, JobState currentJobState, int targetJobHeapMemoryMb,
      Optional<Integer> containerMemoryMbUnit, boolean assignRoundedUpDiffToHeap);

  /**
   * Compute a {@link JobSize}, given the current {@link JobState} and a target total memory size (in MB) for the entire job.
   * @param job jobKey to identify the job
   * @param currentJobState current state of the job
   * @param targetJobMemoryMb target total memory (in MB) of the job
   * @param containerMemoryMbUnit The unit (in MB) to use if rounding-up of containerMemoryMb is desired, empty if
   * rounding-up is not desired. The rounded-up diff is not assigned to heap.
   * @return computed job size
   */
  JobSize resizeToTargetTotalMemSize(JobKey job, JobState currentJobState, int targetJobMemoryMb,
      Optional<Integer> containerMemoryMbUnit);

  /**
   * Compute a {@link JobSize} given the current {@link JobState} and a target total number of threads that should be
   * allotted to the job.
   * @param job jobKey to identify the job
   * @param currentJobState current state of the job
   * @param targetNumThreads total number of threads for the job
   * @return
   */
  JobSize resizeToTargetTotalNumThreads(JobKey job, JobState currentJobState, int targetNumThreads);

  /**
   * Compute a {@link JobSize} given the current {@link JobState} and a target number of vcores for a job
   * @param job jobKey to identify the job
   * @param currentJobState current state of the job
   * @param newTotalNumVcores target number of vcores for this job.
   * @return
   */
  JobSize resizeToTargetVcores(JobKey job, JobState currentJobState, int newTotalNumVcores);

}
