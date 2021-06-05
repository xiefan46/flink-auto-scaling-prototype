package com.linkedin.asc.policy;

import com.linkedin.asc.datapipeline.DataPipeline;
import com.linkedin.asc.model.SizingAction;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestCPUScaleDownPolicy extends AbstractCPUScalingPolicyTest {

  private static final double SCALE_DOWN_MARGIN_FRACTION = 0.1;
  private static final double SCALE_DOWN_TRIGGER_FACTOR = 0.2;


  private Policy policy;

  @Override
  protected Map<String, String> configOverride() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("asc.policy.cpuscaledown.window.ms", MOCK_SCALE_UP_WINDOW_SIZE_MINUTES * MINUTE + "");
    return configMap;
  }

  @Before
  public void init() {
    policy = new CPUScaleDownPolicy(resizer, SCALE_DOWN_TRIGGER_FACTOR, SCALE_DOWN_MARGIN_FRACTION, ascConfig.getMetricWindowSizeForCPUScaleDown());
    policy.initialize(WHITELIST, Duration.ofMinutes(2));
  }

  @Test
  public void testWhitelist() {
    DataPipeline dataPipeline = getMockedDatapipeline(CONTAINER_NUM_VCORE * SCALE_DOWN_TRIGGER_FACTOR - 0.1, 10);
    Assert.assertFalse(policy.apply(PROD_JOB_KEY, dataPipeline).isPresent());
    Assert.assertTrue(policy.apply(TEST_JOB_KEY, dataPipeline).isPresent());
  }

  @Test
  public void testScaleDown() {
    int containerNumVcore = 30;
    double processVcoreUsage = containerNumVcore * SCALE_DOWN_TRIGGER_FACTOR - 0.1;
    double targetVcoresPerContainer = processVcoreUsage * (1 - SCALE_DOWN_MARGIN_FRACTION);
    DataPipeline dataPipeline = getMockedDatapipeline(processVcoreUsage, containerNumVcore);
    Optional<SizingAction> action = policy.apply(TEST_JOB_KEY, dataPipeline);
    Assert.assertTrue(action.isPresent());
    Assert.assertEquals(action.get().getType(), SizingAction.Type.JOB_VCORE_SCALEDOWN);
    Assert.assertTrue(
        Math.abs((int) (Math.floor(targetVcoresPerContainer)) - action.get().targetJobSize.getContainerNumCores())
            <= 1);
  }

  @Test
  public void testNumVcoreResetToOne() {
    DataPipeline dataPipeline = getMockedDatapipeline(0, 100);
    Optional<SizingAction> action = policy.apply(TEST_JOB_KEY, dataPipeline);
    Assert.assertTrue(action.isPresent());
    Assert.assertEquals(1, action.get().targetJobSize.getContainerNumCores());
  }

  @Test
  public void testNullWindow() {
    DataPipeline dataPipeline = getMockedDatapipelineForNullWindow(100);
    Optional<SizingAction> action = policy.apply(TEST_JOB_KEY, dataPipeline);
    Assert.assertFalse(action.isPresent());
  }



}
