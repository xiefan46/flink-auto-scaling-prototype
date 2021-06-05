package org.apache.flink.policy;


import org.apache.flink.config.ASCConfig;
import org.apache.flink.config.MapConfig;
import org.apache.flink.datapipeline.DataPipeline;
import org.apache.flink.model.JobKey;
import org.apache.flink.model.JobState;
import org.apache.flink.model.TimeWindow;
import org.apache.flink.model.functions.MaxFunc;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.testng.annotations.BeforeClass;


public class AbstractTest {

  protected static final int MAX_CONTAINER_MEMORY_MB = 8 * 1024;
  protected static final int MAX_CONTAINER_NUM_THREADS = 16;
  protected static final int MAX_JOB_MEMORY_MB = 100 * 1024;
  protected static final int MAX_NUM_VCORES_PER_CONTAINER = 50;
  protected static final int MAX_NUM_VCORES_PER_JOB = 2048;

  protected static final Pattern WHITELIST = Pattern.compile(".*test.*");
  protected static final String TEST_JOB = "samza-test-job";
  protected static final String PROD_JOB = "samza-prod-job";
  protected static JobKey TEST_JOB_KEY;
  protected static JobKey PROD_JOB_KEY;
  protected ASCConfig ascConfig;
  protected JobState testJobState;

  @BeforeClass
  public void setup() {
    this.ascConfig = getMockSamzaASCConfig(configOverride());
  }

  protected ASCConfig getMockSamzaASCConfig(Map<String, String> configOverride) {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("asc.policy.stateless.maxcontainermb", MAX_CONTAINER_MEMORY_MB + "");
    configMap.put("asc.policy.stateless.maxcontainernumthreads", MAX_CONTAINER_NUM_THREADS + "");
    configMap.put("asc.policy.stateless.maxjobmemorymb", MAX_JOB_MEMORY_MB + "");
    configMap.put("asc.policy.stateless.maxnumvcorespercontainer", MAX_NUM_VCORES_PER_CONTAINER + "");
    configMap.put("asc.policy.stateless.maxnumvcoresperjob", MAX_NUM_VCORES_PER_JOB + "");
    if (configOverride != null) {
      configMap.putAll(configOverride);
    }
    return new ASCConfig(new MapConfig(configMap));
  }

  //Override this method if you have some customized configs for your test case
  protected Map<String, String> configOverride() {
    return new HashMap<>();
  }

  protected static DataPipeline getMockedDatapipeline(TimeWindow timeWindow, JobState testJobState) {
    return null;
  }

  protected static TimeWindow getTimeWindow(double value, Duration length) {
    double metricValue = value;
    TimeWindow timeWindow = new TimeWindow(length);
    Instant now = Instant.now();
    for (int i = 1; i <= length.toMinutes() * 2; i++) {
      // adding one value per minute, such that metricValue = value
      timeWindow.addMetric(Instant.ofEpochMilli(now.toEpochMilli() + i * 60 * 1000), metricValue, MaxFunc.MAX_FUNC);
    }

    return timeWindow;
  }
}
