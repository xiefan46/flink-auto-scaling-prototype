package org.apache.flink.asc.policy;


import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.datapipeline.DataPipeline;
import org.apache.flink.model.SizingAction;
import org.apache.flink.policy.Policy;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CPUScaleUpPolicyTest extends TestCPUScalingPolicy {

  private static final double scaleUpTriggerFactor = 2.0;
  private static final double scaleUpMarginFraction = 0.1;

  private Policy policy;

  @Override
  protected Map<String, String> configOverride() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("samzaasc.policy.cpuscaleup.window.ms", MOCK_SCALE_UP_WINDOW_SIZE_MINUTES * MINUTE + "");
    return configMap;
  }

  @BeforeClass
  public void init() {

    policy.initialize(WHITELIST, Duration.ofMinutes(2));

  }

  @Test
  public void testWhitelist() {
    DataPipeline dataPipeline = getMockedDatapipeline(CONTAINER_NUM_VCORE * scaleUpTriggerFactor + 0.1, 10);
    Assert.assertFalse(policy.apply(PROD_JOB_KEY, dataPipeline).isPresent());
    Assert.assertTrue(policy.apply(TEST_JOB_KEY, dataPipeline).isPresent());
  }

  @Test
  public void testScaleUp() {
    //within the margin, do not do scale up
    DataPipeline dataPipeline1 =
        getMockedDatapipeline(CONTAINER_NUM_VCORE * scaleUpTriggerFactor - 0.1, CONTAINER_NUM_VCORE);
    Assert.assertFalse(policy.apply(TEST_JOB_KEY, dataPipeline1).isPresent());
    //outside the margin, do scale up
    double processVcoreUsage = CONTAINER_NUM_VCORE * scaleUpTriggerFactor + 0.1;
    double targetVcoresPerContainer = processVcoreUsage * (1 + scaleUpMarginFraction);
    DataPipeline dataPipeline2 = getMockedDatapipeline(processVcoreUsage, CONTAINER_NUM_VCORE);
    Optional<SizingAction> action = policy.apply(TEST_JOB_KEY, dataPipeline2);
    Assert.assertTrue(action.isPresent());
    Assert.assertEquals(action.get().getType(), SizingAction.Type.JOB_VCORE_SCALEUP);
    Assert.assertTrue(
        Math.abs((int) (Math.floor(targetVcoresPerContainer)) - action.get().targetJobSize.getContainerNumCores())
            <= 1);
  }

  @Test
  public void testExceedMaximum() {
    //larger than the maximum, reset to maximum
    DataPipeline dataPipeline = getMockedDatapipeline(MAX_NUM_VCORES_PER_CONTAINER + 1, 10);
    Optional<SizingAction> action = policy.apply(TEST_JOB_KEY, dataPipeline);
    Assert.assertTrue(action.isPresent());
    Assert.assertEquals(MAX_NUM_VCORES_PER_CONTAINER, action.get().getTargetJobSize().getContainerNumCores());
  }

  @Test
  public void testNullWindow() {
    DataPipeline dataPipeline = getMockedDatapipelineForNullWindow(MAX_NUM_VCORES_PER_CONTAINER);
    Optional<SizingAction> action = policy.apply(TEST_JOB_KEY, dataPipeline);
    Assert.assertFalse(action.isPresent());
  }

}
