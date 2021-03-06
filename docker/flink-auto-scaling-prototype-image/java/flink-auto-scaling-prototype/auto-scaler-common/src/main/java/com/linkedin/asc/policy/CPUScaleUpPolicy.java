package com.linkedin.asc.policy;
import com.linkedin.asc.model.SizingAction;
import com.linkedin.asc.policy.resizer.Resizer;
import java.time.Duration;


/**
 * Extends {@link CPUScalingPolicy} and implements cpu scale up specific logic
 */
public class CPUScaleUpPolicy extends CPUScalingPolicy {

  //Scale up if actualVcoreUsage >= configVcoreNum * cpuScaleUpTriggerFactor
  private  double cpuScaleUpTriggerFactor;

  //When decide to do scale up, scale config value to actual_vcore_used * (1 + cpuScaleUpMarginFraction)
  private  double cpuScaleUpMarginFraction;

  public CPUScaleUpPolicy(Resizer resizer, double scaleUpTriggerFactor, double scaleUpMarginFraction, Duration metricWindowSizeForScaleUp) {
    super(resizer, metricWindowSizeForScaleUp);
    this.cpuScaleUpTriggerFactor = scaleUpTriggerFactor;
    this.cpuScaleUpMarginFraction = scaleUpMarginFraction;
  }

  //Scale up if actualVcoreUsage >= configVcoreNum * cpuScaleUpTriggerFactor
  @Override
  protected boolean isScalingRequired(int configVcoreNum, double actualVcoreUsage) {
    return actualVcoreUsage >= marginUpperOrLowerBound(configVcoreNum);
  }

  //Margin upper for scale up
  @Override
  protected double marginUpperOrLowerBound(int configVcoreNum) {
    return configVcoreNum * cpuScaleUpTriggerFactor;
  }

  //Calculate the desired vcore. For Scale up: scale vcore number to actualVcoreUsage * (1 + this.cpuScaleUpMarginFraction)
  @Override
  protected int getTargetVcoreNumPerContainer(double actualVcoreUsage) {
    return (int) Math.ceil(actualVcoreUsage * (1 + this.cpuScaleUpMarginFraction));
  }

  /**
   * The {@link SizingAction.Type} to populate in the actions generated by this class.
   * @return
   */
  @Override
  protected SizingAction.Type getActionType() {
    return SizingAction.Type.JOB_VCORE_SCALEUP;
  }
}
