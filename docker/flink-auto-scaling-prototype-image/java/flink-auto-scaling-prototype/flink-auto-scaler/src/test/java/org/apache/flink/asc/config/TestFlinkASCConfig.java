package org.apache.flink.asc.config;

import com.linkedin.asc.config.MapConfig;
import java.time.Duration;
import java.util.regex.Pattern;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestFlinkASCConfig {

  private String path = "src/test/resources/flink-asc-config.properties";

  ParameterTool parameter;

  @Before
  public void testParameterTool() throws Exception{
    parameter = ParameterTool.fromPropertiesFile(path);
  }

  @Test
  public void testFlinkASCConfig() throws Exception {
    FlinkASCConfig flinkASCConfig = new FlinkASCConfig(new MapConfig(parameter.toMap()));
    Assert.assertEquals(Pattern.compile(".*").toString(), flinkASCConfig.getWhitelistForCPUScaleUpPolicy().toString());
    Assert.assertEquals(Duration.ofMillis(3600000), flinkASCConfig.getMetricWindowSizeForCPUScaleUp());
    Assert.assertEquals(1.1, flinkASCConfig.getCPUScalingPolicyScaleUpTriggerFactor(), 0.0001);
  }


}
