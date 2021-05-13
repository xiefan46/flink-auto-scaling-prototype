package com.linkedin.asc.policy;

import com.linkedin.asc.datapipeline.DataPipeline;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobSize;
import com.linkedin.asc.model.JobState;
import com.linkedin.asc.model.SizingAction;
import com.linkedin.asc.model.TimeWindow;
import com.linkedin.asc.policy.resizer.Resizer;
import java.time.Duration;
import java.util.Optional;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.linkedin.asc.util.Utils.*;



/**
 * Policy implementation to control the num of vcores per container based on
 * comparing the number of actual vcore used with the number of vcore config.
 * Do vcore scale up/down action if necessary.
 * Design docs : https://docs.google.com/document/d/13Matt0ECp25eMrEYV_TZwaolY9sz6TeCrq40enBh9lo/edit?usp=sharing
 */
public abstract class CPUScalingPolicy extends Policy {

  protected  Logger logger;

  // Resizer to compute sizing-parameters when increasing/decreasing memory (heap or non-heap)
  private Resizer resizer;

  private  String policyName;

  //Window size for cpu scale up or scale down
  private  Duration windowSize;

  protected CPUScalingPolicy(Resizer resizer, Duration windowSize) {
    this.windowSize = windowSize;
    this.logger = LoggerFactory.getLogger(this.getClass());
    this.policyName = this.getClass().getName();
    this.resizer = resizer;
  }

  private static final Duration INITIALIZATION_PERIOD = Duration.ofSeconds(30);

  @Override
  public void initialize(Pattern jobNameWhitelist, Duration maxStaleness) {
    super.initialize(jobNameWhitelist, maxStaleness);
  }

  @Override
  protected Optional<SizingAction> doApply(JobKey job, DataPipeline dataPipeline) {

    JobState jobState = dataPipeline.getCurrentJobState(job);
    JobSize currentSize = jobState.getJobSize();
    JobSize targetSize = currentSize;

    logger.info("Check {} for job: {}, job current size : {}", policyName, job, currentSize);
    TimeWindow timeWindow = dataPipeline.getProcessVcoreUsageMetricWindow(job);
    TimeWindow subWindow = timeWindow == null ? null : timeWindow.getTrailingWindow(windowSize);
    if (isValidWindow(job, jobState, subWindow, INITIALIZATION_PERIOD)) {
      int configVcore = currentSize.getContainerNumCores();
      double vcoreUsage = subWindow.getAverage();
      if (isScalingRequired(configVcore, vcoreUsage)) {
        vcoreUsage = maybeBoundVcoreUsageWithStateRestorationMetrics(vcoreUsage, job, jobState, dataPipeline);
        int targetVcoreNumPerContainer = getTargetVcoreNumPerContainer(vcoreUsage);
        int targetTotalNumVcore = targetVcoreNumPerContainer * jobState.getJobSize().getContainerCount();
        logger.info("{} required. Job: {}, Config Vcore : {} Vcore Usage : {} Scaling trigger upper/lower bound : {}",
            policyName, job, configVcore, vcoreUsage, marginUpperOrLowerBound(configVcore));
        logger.info("Setting num vcore of job: {} to {} ", job, targetTotalNumVcore);
        targetSize = resizer.resizeToTargetVcores(job, jobState, targetTotalNumVcore);
      } else {
        logger.info("{} is not required. Job: {}, vcore Usage : {} Scaling trigger upper/lower bound : {}", policyName,
            job, vcoreUsage, marginUpperOrLowerBound(configVcore));
      }
    } else {
      logger.debug("Job: {}, invalid window", job);
    }

    // if target-size is different from current size, we suggest the sizingAction
    if (!targetSize.equals(currentSize)) {
      SizingAction sizingAction = new SizingAction(job, currentSize, targetSize, getActionType());
      logger.info("Requesting action to set size of job: {} to {}, current size: {}", sizingAction.jobKey,
          sizingAction.targetJobSize, jobState.getJobSize());
      return Optional.of(sizingAction);
    } else {
      logger.info("Taking no action on job: {}", job);
      return Optional.empty();
    }
  }

  // By default we do not bound vcore usage with state restoration metrics
  protected double maybeBoundVcoreUsageWithStateRestorationMetrics(double vcoreUsage, JobKey job, JobState jobState,
      DataPipeline dataPipeline) {
    return vcoreUsage;
  }

  //Check if scale up/down is needed
  protected abstract boolean isScalingRequired(int configVcoreNum, double actualVcoreUsage);

  //Calculate the upper/lower bound for scale up/down.
  protected abstract double marginUpperOrLowerBound(int configVcoreNum);

  /*
    Calculate the desired vcore.
   */
  protected abstract int getTargetVcoreNumPerContainer(double actualVcoreUsage);

  /**
   * Return the {@link SizingAction.Type} to use for actions generated.
   * @return
   */
  protected abstract SizingAction.Type getActionType();
}
