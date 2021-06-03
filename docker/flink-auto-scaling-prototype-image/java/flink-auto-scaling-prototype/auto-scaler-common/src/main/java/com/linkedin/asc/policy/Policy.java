package com.linkedin.asc.policy;

import com.linkedin.asc.datapipeline.DataPipeline;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.SizingAction;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Policy {

  private static final Logger LOG = LoggerFactory.getLogger(Policy.class.getName());

  // List of jobs on which this policy applies
  private Pattern jobNameWhitelist;

  // Max amount of time a job's last-known-ts can be from current to allow policy-application
  private Duration maxStaleness;

  // whitelist to disable lag-based scale-up and all scale-down sizing actions during the state restore stage of stateful jobs.
  private Pattern disableSizingStateRestoringStatefulJobsWhitelist;

  /**
   * See {@link #initialize(Pattern, Duration, Pattern)}
   */
  public void initialize(Pattern whitelist, Duration maxStaleness) {
    initialize(whitelist, maxStaleness, Pattern.compile(".^"));
  }

  /**
   * Initialize the policy with the provided whitelist.
   * @param whitelist The set of apps on which this policy is to be applied.
   * @param maxStaleness max staleness of the job's last known timestamp for the policy to be applicable
   * @param disableSizingStateRestoringStatefulJobsWhitelist whitelist to disable lag-based scale-up and all scale-down
   *                                                         sizing actions during the state restore stage of stateful jobs
   */
  public void initialize(
      Pattern whitelist, Duration maxStaleness, Pattern disableSizingStateRestoringStatefulJobsWhitelist) {
    this.jobNameWhitelist = whitelist;
    this.maxStaleness = maxStaleness;
    this.disableSizingStateRestoringStatefulJobsWhitelist = disableSizingStateRestoringStatefulJobsWhitelist;
  }

  /**
   * Apply the policy on the given job using any data from the pipeline that may be required.
   * @param jobKey the job on which the policy is to be applied.
   * @param dataPipeline data pipeline
   * @return A sizing option if one is to be taken, none otherwise
   */
  public final Optional<SizingAction> apply(JobKey jobKey, DataPipeline dataPipeline) {
    if (!shouldApplyPolicy(jobKey, dataPipeline)) {
      return Optional.empty();
    }
    return doApply(jobKey, dataPipeline);
  }

  /**
   * Abstract method that encapsulates any policy-specific logic.
   * @param jobKey the job on which the policy is to be applied.
   * @param dataPipeline data pipeline
   * @return A sizing option if one is to be taken, none otherwise
   */
  protected abstract Optional<SizingAction> doApply(JobKey jobKey, DataPipeline dataPipeline);

  /**
   * Check if the job's name matches the job-name whitelist, and the job's last known timestamp is within bound.
   */
  private boolean shouldApplyPolicy(JobKey jobKey, DataPipeline dataPipeline) {
    if (jobNameWhitelist.matcher(jobKey.getJobName()).matches()) {
      Instant jobLastKnownTimestamp = Instant.ofEpochMilli(dataPipeline.getCurrentJobState(jobKey).getLastTime());
      Duration staleness = Duration.between(jobLastKnownTimestamp, Instant.now());
      LOG.info("Job: {}, staleness: {}, maxStaleness: {}", jobKey, staleness, maxStaleness);
      return  staleness.compareTo(maxStaleness) <= 0;
    }
    return false;
  }
}