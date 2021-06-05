package com.linkedin.asc.policy;

import com.linkedin.asc.model.SizingAction;
import com.linkedin.asc.policy.resizer.Resizer;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Extends {@link CPUScalingPolicy} and implements cpu scale down specific logic
 */
public class CPUScaleDownPolicy extends CPUScalingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(CPUScaleDownPolicy.class);

  //Scale down if actualVcoreUsage <= configVcoreNum *  cpuScaleDownTriggerFactor
  private double cpuScaleDownTriggerFactor;

  //When decide to do scale down, scale config value to actual_vcore_used * (1 - cpuScaleDownMarginFraction)
  private double cpuScaleDownMarginFraction;

  public CPUScaleDownPolicy(Resizer resizer, double scaleDownTriggerFactor,
      double scaleDownMarginFraction, Duration metricWindowSizeForCPUScaleDown) {
    super(resizer, metricWindowSizeForCPUScaleDown);
    this.cpuScaleDownTriggerFactor = scaleDownTriggerFactor;
    this.cpuScaleDownMarginFraction = scaleDownMarginFraction;
  }


  //Scale down if actualVcoreUsage <= configVcoreNum * cpuScaleDownTriggerFactor
  @Override
  protected boolean isScalingRequired(int configVcoreNum, double actualVcoreUsage) {
    return actualVcoreUsage <= marginUpperOrLowerBound(configVcoreNum);
  }

  //Calculate the lower bound for scale down.
  @Override
  protected double marginUpperOrLowerBound(int configVcoreNum) {
    return configVcoreNum * cpuScaleDownTriggerFactor;
  }

  /*
    Calculate the desired vcore number for CPU scaling.
    For Scale down: scale vcore number to actualVcoreUsage * (1 - this.cpuScaleDownMarginFraction)
   */
  @Override
  protected int getTargetVcoreNumPerContainer(double actualVcoreUsage) {
    return (int) Math.ceil(actualVcoreUsage * (1 - this.cpuScaleDownMarginFraction));
  }

  /**
   * Return the action-type to use for actions generated.
   * @return
   */
  @Override
  protected SizingAction.Type getActionType() {
    return SizingAction.Type.JOB_VCORE_SCALEDOWN;
  }

}
