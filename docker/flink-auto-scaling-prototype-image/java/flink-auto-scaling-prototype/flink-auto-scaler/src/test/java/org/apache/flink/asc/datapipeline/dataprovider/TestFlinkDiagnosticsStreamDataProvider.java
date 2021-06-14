package org.apache.flink.asc.datapipeline.dataprovider;

import com.linkedin.asc.config.ASCConfig;
import com.linkedin.asc.config.MapConfig;
import com.linkedin.asc.datapipeline.DataPipeline;
import com.linkedin.asc.datapipeline.dataprovider.DiagnosticsStreamDataProvider;
import com.linkedin.asc.model.DiagnosticsMessage;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobState;
import com.linkedin.asc.model.TimeWindow;
import com.linkedin.asc.model.functions.MaxFunc;
import com.linkedin.asc.store.InternalInMemoryStore;
import com.linkedin.asc.store.KeyValueStore;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.flink.diagnostics.model.FlinkDiagnosticsMessage;
import org.apache.flink.diagnostics.model.FlinkMetricsHeader;
import org.apache.flink.diagnostics.model.FlinkMetricsSnapshot;
import org.apache.flink.metrics.Gauge;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.testng.Assert;


/**
 * TODO: Implement a AbstractDiagnosticsStreamDataProvideTest and extract common test cases for Flink and Samza
 */
public class TestFlinkDiagnosticsStreamDataProvider {

  private final static String JOB_NAME = "Testjob";
  private final static String ATTEMPT_ID = "attempt-01";
  private final static String ATTEMPT_ID2 = "attempt-02";
  private final static String TM_ID = "tm-01";
  private final static String TM_ID2 = "tm-02";
  private final static long TIMESTAMP = System.currentTimeMillis();
  private static final Duration THIRTY_MIN_WINDOW_LENGTH = Duration.ofMinutes(30);
  private static final int DEFAULT_COUNT_WINDOW_LENGTH = 3;
  private static final long MINUTE = 1000 * 60;
  private static final int DEFAULT_NUM_VCORES = 100;
  private static final Random RAND = new Random(System.currentTimeMillis());

  private final KeyValueStore<JobKey, JobState> jobStateStore = new InternalInMemoryStore<>();

  private final KeyValueStore<String, LinkedList<String>> jobAttemptsStore = new InternalInMemoryStore<>();

  // KV store for cpu related metrics
  private final KeyValueStore<JobKey, TimeWindow> processVcoreUsageMetricStore = new InternalInMemoryStore<>();

  private final Duration processVCoreUsageWindowSize = Duration.ofMinutes(30);

  private DiagnosticsStreamDataProvider diagnosticsStreamDataProvider;

  private DataPipeline dataPipeline;

  private ASCConfig getMockASCConfig() {
    Map<String, String> map = new HashMap<>();
    map.put("asc.metric.process.vcore.usage.whitelist", "");
    map.put("asc.policy.bound-job-size-with-state-restoration-info.whitelist", ".*");
    map.put("asc.policy.remoteio.diag.message.gathering.window.ms", "60000");
    map.put("asc.policy.lag.window.ms", "60000");
    map.put("asc.policy.state-restoration.min.duration.ms", "0");
    map.put("asc.coverage", ".*");
    return new ASCConfig(new MapConfig(map));
  }

  private Map<String, String> getMockJobConfigMap(String path) throws Exception {
    InputStream in = null;
    try {
      in = getClass().getClassLoader().getResourceAsStream(path);
      Properties prop = new Properties();
      prop.load(in);
      return new HashMap<String, String>((Map) prop);
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  @Before
  public void setup() throws Exception {
    dataPipeline = Mockito.mock(DataPipeline.class);
    diagnosticsStreamDataProvider = createDiagnosticsStreamDataProvider();
  }

  @Test
  public void testNullDiagnosticsMessage() {
    FlinkDiagnosticsStreamDataProvider diagnosticsStreamDataProvider = createDiagnosticsStreamDataProvider();
    diagnosticsStreamDataProvider.receiveData(null, dataPipeline);
    Assert.assertTrue(diagnosticsStreamDataProvider.getAllJobsState().isEmpty());
  }

  @Test
  public void testSingleValidDiagnosticsMessage() {

    DiagnosticsMessage diagnosticsMessage = createFlinkTaskManagerDiagnosticsMessage(JOB_NAME, ATTEMPT_ID, TM_ID, true, 0.1, TIMESTAMP);
    // Setting timestamp as the eventTime in the IME (used for parsing)
    diagnosticsStreamDataProvider.receiveData(diagnosticsMessage, dataPipeline);

    Map<JobKey, JobState> jobs = diagnosticsStreamDataProvider.getAllJobsState();
    Assert.assertEquals(jobs.size(), 1);
    JobKey jobKey = new JobKey(JOB_NAME, ATTEMPT_ID);

    Assert.assertEquals(Collections.singleton(jobKey), jobs.keySet());

    Assert.assertEquals(TIMESTAMP, jobs.get(jobKey).getFirstTime());
    Assert.assertEquals(TIMESTAMP, jobs.get(jobKey).getLastTime());

  }

  @Test
  public void testAttemptProcessingAcrossMetricsSnapshots() {

    DiagnosticsMessage diagnosticsMessage = createFlinkTaskManagerDiagnosticsMessage(JOB_NAME, ATTEMPT_ID, TM_ID,
        true, 0.1, TIMESTAMP);

    //New attempt id
    diagnosticsStreamDataProvider.receiveData(diagnosticsMessage, dataPipeline);
    DiagnosticsMessage diagnosticsMessage2 = createFlinkTaskManagerDiagnosticsMessage(JOB_NAME, ATTEMPT_ID2, TM_ID,
        true, 0.1, TIMESTAMP + 2);
    diagnosticsStreamDataProvider.receiveData(diagnosticsMessage2, dataPipeline);

    //Test out-of-order processing
    DiagnosticsMessage diagnosticsMessage3 = createFlinkTaskManagerDiagnosticsMessage(JOB_NAME, ATTEMPT_ID, TM_ID,
        true, 0.1, TIMESTAMP + 1);
    diagnosticsStreamDataProvider.receiveData(diagnosticsMessage3, dataPipeline);

    Assert.assertTrue(diagnosticsStreamDataProvider.getLatestAttempts()
            .equals(Collections.singleton(new JobKey(JOB_NAME, ATTEMPT_ID2))), "Should return a map with one row that is the jobKey of the latest job");
  }

  @Test
  public void testStoreDeletion() {
    // First we receive a metricsMessage with auto-sizing enabled
    DiagnosticsMessage diagnosticsMessage = createFlinkTaskManagerDiagnosticsMessage(JOB_NAME, ATTEMPT_ID, TM_ID,
        true, 0.1, TIMESTAMP);

    diagnosticsStreamDataProvider.receiveData(diagnosticsMessage, dataPipeline);

    Map<JobKey, JobState> jobs = diagnosticsStreamDataProvider.getAllJobsState();
    JobKey jobKey = new JobKey(JOB_NAME, ATTEMPT_ID);
    Assert.assertEquals(Collections.singleton(jobKey), jobs.keySet());
    Assert.assertFalse(diagnosticsStreamDataProvider.getLatestAttempts().isEmpty());
    Assert.assertNotNull(diagnosticsStreamDataProvider.getProcessVcoreUsageMetricWindow(jobKey));

    // We receive a metricsMessage with auto-sizing disabled
    diagnosticsMessage = createFlinkTaskManagerDiagnosticsMessage(JOB_NAME, ATTEMPT_ID2, TM_ID,
        false, 0.1, TIMESTAMP + 1);
    diagnosticsStreamDataProvider.receiveData(diagnosticsMessage, dataPipeline);

    // All state stores should now be empty
    Assert.assertTrue(diagnosticsStreamDataProvider.getAllJobsState().isEmpty());
    Assert.assertTrue(diagnosticsStreamDataProvider.getLatestAttempts().isEmpty());
    Assert.assertNull(diagnosticsStreamDataProvider.getProcessVcoreUsageMetricWindow(jobKey));
  }

  //TODO: implement this test case when we figure out a way to detect stale attempts in Flink
  @Test
  public void testStaleInput() {

  }

  /**
   * Test whether {@link DiagnosticsStreamDataProvider} can record cpu related metrics correctly
   * in its {@link TimeWindow}
   */
  @Test
  public void testGetCPURelatedMetrics() {

    long startTime = System.currentTimeMillis();

    TimeWindow actualWindow = new TimeWindow(THIRTY_MIN_WINDOW_LENGTH);
    int T = 30;
    for (int i = 0; i < T; i++) {
      long time = startTime + i * MINUTE;
      double randProcessCPUUsage = RAND.nextDouble() * 100;
      actualWindow.addMetric(Instant.ofEpochMilli(time), randProcessCPUUsage / 100.0 * DEFAULT_NUM_VCORES,
          MaxFunc.MAX_FUNC);
      DiagnosticsMessage diagnosticsMessage = createFlinkTaskManagerDiagnosticsMessage(JOB_NAME, ATTEMPT_ID, TM_ID,
          true, randProcessCPUUsage, time);
      //Metrics from job manager should not be used
      DiagnosticsMessage jobManagerDiagnosticsMessage = createFlinkJobManagerDiagnosticsMessage(JOB_NAME, ATTEMPT_ID, TM_ID,
          true, randProcessCPUUsage, time);
      diagnosticsStreamDataProvider.receiveData(diagnosticsMessage, dataPipeline);
    }
    Map<JobKey, JobState> jobStates = diagnosticsStreamDataProvider.getAllJobsState();
    org.testng.Assert.assertEquals(jobStates.size(), 1);
    for (JobKey key : jobStates.keySet()) {
      TimeWindow processVcoresWindow = diagnosticsStreamDataProvider.getProcessVcoreUsageMetricWindow(key);
      Assert.assertEquals((int) processVcoresWindow.getAverage(), (int) actualWindow.getAverage());
    }
  }

  private FlinkDiagnosticsStreamDataProvider createDiagnosticsStreamDataProvider() {

    FlinkDiagnosticsStreamDataProvider flinkDiagnosticsStreamDataProvider =
        new FlinkDiagnosticsStreamDataProvider(jobStateStore, jobAttemptsStore, processVcoreUsageMetricStore,
            processVCoreUsageWindowSize);
    return flinkDiagnosticsStreamDataProvider;
  }

  private FlinkDiagnosticsMessage createFlinkTaskManagerDiagnosticsMessage(String jobName, String attemptId, String containerName,
      boolean autoScalingEnableddouble, double cpuUsageRate, long timestamp) {
    FlinkMetricsHeader flinkMetricsHeader = getMockFlinkMetricsHeader(jobName, attemptId, containerName);
    FlinkMetricsSnapshot flinkMetricsSnapshot = getTaskmanagerMetricsSnapshot(cpuUsageRate);
    FlinkDiagnosticsMessage flinkDiagnosticsMessage = new FlinkDiagnosticsMessage(flinkMetricsHeader, flinkMetricsSnapshot, autoScalingEnableddouble, timestamp);
    return flinkDiagnosticsMessage;
  }

  private FlinkDiagnosticsMessage createFlinkJobManagerDiagnosticsMessage(String jobName, String attemptId, String containerName,
      boolean autoScalingEnableddouble, double cpuUsageRate, long timestamp) {
    FlinkMetricsHeader flinkMetricsHeader = getMockFlinkMetricsHeader(jobName, attemptId, containerName);
    FlinkMetricsSnapshot flinkMetricsSnapshot = getJobmanagerMetricsSnapshot(cpuUsageRate);
    FlinkDiagnosticsMessage flinkDiagnosticsMessage = new FlinkDiagnosticsMessage(flinkMetricsHeader, flinkMetricsSnapshot, autoScalingEnableddouble, timestamp);
    return flinkDiagnosticsMessage;
  }

  private FlinkMetricsHeader getMockFlinkMetricsHeader(String jobName, String attemptId, String containerName) {
    FlinkMetricsHeader flinkMetricsHeader = new FlinkMetricsHeader();
    flinkMetricsHeader.put("<job_name>", jobName);
    flinkMetricsHeader.put("<job_id>", attemptId);
    flinkMetricsHeader.put("<tm_id>", containerName);
    return flinkMetricsHeader;
  }

  /**
   * MetricsSnapshot structure:
   *
   * root
   *  -taskmanager
   *    - Status
   *      - CPU
   *        - Load
   *        - Time
   */
  private FlinkMetricsSnapshot getTaskmanagerMetricsSnapshot(double cpuUsageRate) {
    Map<String, Object> metricsGroup = new HashMap<>();
    String[] groupNames = new String[] {"taskmanager", "Status", "JVM", "CPU" };
    Map<String, Object> cpuMetricsGroup = FlinkMetricsSnapshot.visitMetricGroupMap(metricsGroup, groupNames, true);
    cpuMetricsGroup.put("Load", (Gauge<Double>) () -> cpuUsageRate);
    cpuMetricsGroup.put("Time", (Gauge<Double>) () -> -1.0);
    return FlinkMetricsSnapshot.convertToMetricsSnapshot(metricsGroup);
  }

  private FlinkMetricsSnapshot getJobmanagerMetricsSnapshot(double cpuUsageRate) {
    Map<String, Object> metricsGroup = new HashMap<>();
    String[] groupNames = new String[] {"jobmanager", "Status", "JVM", "CPU" };
    Map<String, Object> cpuMetricsGroup = FlinkMetricsSnapshot.visitMetricGroupMap(metricsGroup, groupNames, true);
    cpuMetricsGroup.put("Load", (Gauge<Double>) () -> cpuUsageRate);
    cpuMetricsGroup.put("Time", (Gauge<Double>) () -> -1.0);
    return FlinkMetricsSnapshot.convertToMetricsSnapshot(metricsGroup);
  }

}
