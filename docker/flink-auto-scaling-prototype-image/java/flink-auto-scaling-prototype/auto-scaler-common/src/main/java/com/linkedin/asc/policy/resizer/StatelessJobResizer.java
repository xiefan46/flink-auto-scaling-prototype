package com.linkedin.asc.policy.resizer;

import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobSize;
import com.linkedin.asc.model.JobState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Resizer impl for stateless jobs that increases native (or heap) memory while keeping heap (or native respectively) constant,
 * while increasing container-count, heapsize, etc as required and accounting for safeguards and num-tasks.
 * Containers should be merged if the resource usage is below the lower bound.
 */
public class StatelessJobResizer implements Resizer {
  private final static Logger LOG = LoggerFactory.getLogger(StatelessJobResizer.class.getName());

  // Max container size mb for a container
  public final int maxContainerMemoryMb;

  // cluster-safeguard limit on the total memory of a single job
  public final int maxJobMemoryMb;

  //Max number of vcores for a container.
  public final int maxNumVcoresPerContainer;

  //Max number of vcores for a job.
  public final int maxNumVcoresPerJob;

  public StatelessJobResizer(int maxContainerMemoryMb, int maxJobMemoryMb,
      int maxNumVcoresPerContainer, int maxNumVcoresPerJob) {
    this.maxContainerMemoryMb = maxContainerMemoryMb;
    this.maxJobMemoryMb = maxJobMemoryMb;
    this.maxNumVcoresPerContainer = maxNumVcoresPerContainer;
    this.maxNumVcoresPerJob = maxNumVcoresPerJob;
  }

  /**
   * Compute a {@link JobSize} given the current {@link JobState} and a target number of vcores for a job
   */
  @Override
  public JobSize resizeToTargetVcores(JobKey job, JobState currentJobState, int newTotalNumVcores) {
    int containerCount = currentJobState.getJobSize().getContainerCount();
    newTotalNumVcores = applyMaxJobVcoreLimit(job, currentJobState.getJobSize(), newTotalNumVcores);
    int targetVcoresPerContainer = newTotalNumVcores / containerCount;
    if (targetVcoresPerContainer < 1) {
      LOG.info("Job: {}, target vcore number per container: {} smaller than 1, reset to 1 ", job,
          targetVcoresPerContainer);
      targetVcoresPerContainer = 1;
    }
    JobSize currentSize = currentJobState.getJobSize();
    return new JobSize(currentSize.getContainerMb(), targetVcoresPerContainer, currentSize.getContainerCount());
  }


  /**
   * Helper method to check if the job's vcore number exceeds the limit
   */
  private int applyMaxJobVcoreLimit(JobKey job, JobSize currentJobSize, int newTotalNumVcores) {
    int containerCount = currentJobSize.getContainerCount();
    int maxVcoreLimit = Math.min(containerCount * maxNumVcoresPerContainer, maxNumVcoresPerJob);
    // total job vcore number cannot be larger than the limit
    if (newTotalNumVcores > maxVcoreLimit) {
      LOG.warn("Limit on total job vcore: {} reached for job: {}", maxVcoreLimit, job);
      /**
       * If the user manually config more vcores than the limit, we keep the user's config
       *  to avoid decreasing the vcore count in a scale up action.
       */
      newTotalNumVcores = Math.max(currentJobSize.getTotalNumCores(), maxVcoreLimit);
    }
    return newTotalNumVcores;
  }

}
