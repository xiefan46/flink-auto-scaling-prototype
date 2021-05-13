package com.linkedin.asc.policy;

import com.linkedin.asc.config.ASCConfig;
import com.linkedin.asc.config.MapConfig;
import com.linkedin.asc.datapipeline.DataPipeline;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobState;
import com.linkedin.asc.model.TimeWindow;
import com.linkedin.asc.model.functions.MaxFunc;
import com.linkedin.asc.policy.resizer.Resizer;
import com.linkedin.asc.policy.resizer.StatelessJobResizer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.Before;


public class AbstractTest {

  protected static final int MAX_CONTAINER_MEMORY_MB = 8 * 1024;
  protected static final int MAX_CONTAINER_NUM_THREADS = 16;
  protected static final int MAX_JOB_MEMORY_MB = 100 * 1024;
  protected static final int MAX_NUM_VCORES_PER_CONTAINER = 50;
  protected static final int MAX_NUM_VCORES_PER_JOB = 2048;

  protected static final Pattern WHITELIST = Pattern.compile(".*test.*");
  protected static final String TEST_JOB = "samza-test-job";
  protected static final String PROD_JOB = "samza-prod-job";
  protected static JobKey TEST_JOB_KEY = new JobKey(TEST_JOB, "attempt-01");
  protected static JobKey PROD_JOB_KEY = new JobKey(PROD_JOB, "attempt-01");
  protected ASCConfig ascConfig;
  protected JobState testJobState;
  protected Resizer resizer;

  @Before
  public void setup() {
    this.resizer = new StatelessJobResizer(MAX_CONTAINER_MEMORY_MB, MAX_JOB_MEMORY_MB,MAX_NUM_VCORES_PER_CONTAINER, MAX_NUM_VCORES_PER_JOB);
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
