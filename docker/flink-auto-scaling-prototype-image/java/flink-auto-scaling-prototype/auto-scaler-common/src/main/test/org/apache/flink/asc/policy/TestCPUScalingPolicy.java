package org.apache.flink.asc.policy;

import com.linkedin.asc.datapipeline.DataPipeline;
import com.linkedin.asc.model.JobState;
import com.linkedin.asc.model.TimeWindow;
import com.linkedin.asc.model.functions.MaxFunc;
import java.time.Duration;
import java.time.Instant;
import org.mockito.Mockito;


public abstract class TestCPUScalingPolicy extends AbstractTest {
  protected static final long MINUTE = 1000 * 60;
  protected static final int MOCK_SCALE_UP_WINDOW_SIZE_MINUTES = 30;
  protected static final int CONTAINER_NUM_VCORE = 10;

  protected DataPipeline getMockedDatapipeline(double processVcoreUsage, int containerNumVcores) {
    long startTime = Instant.now().toEpochMilli();
    return getMockedDatapipeline(processVcoreUsage, getTestJobStateWithVcoreNum(containerNumVcores, startTime), false);
  }

  protected DataPipeline getMockedDatapipeline(double processVcoreUsage, JobState jobState,
      boolean isStatefulJobRestoringState) {
    DataPipeline dataPipeline = Mockito.mock(DataPipeline.class);
    TimeWindow window = getMockMetricWindow(processVcoreUsage, jobState.getLastTime() + 10 * MINUTE);
    Mockito.when(dataPipeline.getCurrentJobState(Mockito.any())).thenReturn(jobState);
    Mockito.when(dataPipeline.getProcessVcoreUsageMetricWindow(Mockito.any())).thenReturn(window);
    return dataPipeline;
  }

  protected DataPipeline getMockedDatapipelineForNullWindow(int containerNumVcores) {
    DataPipeline dataPipeline = Mockito.mock(DataPipeline.class);
    long startTime = Instant.now().toEpochMilli();
    Mockito.when(dataPipeline.getCurrentJobState(Mockito.any()))
        .thenReturn(getTestJobStateWithVcoreNum(containerNumVcores, startTime));
    Mockito.when(dataPipeline.getProcessVcoreUsageMetricWindow(Mockito.any())).thenReturn(null);
    return dataPipeline;
  }

  protected TimeWindow getMockMetricWindow(double processVcoreUsage, long startTime) {
    TimeWindow window = new TimeWindow(Duration.ofMinutes(MOCK_SCALE_UP_WINDOW_SIZE_MINUTES));
    for (int i = 0; i < MOCK_SCALE_UP_WINDOW_SIZE_MINUTES + 1; i++) {
      window.addMetric(Instant.ofEpochMilli(startTime + i * MINUTE), processVcoreUsage, MaxFunc.MAX_FUNC);
    }
    return window;
  }

  protected JobState getTestJobStateWithVcoreNum(int containerNumVcores, long startTime) {
    return null;
  }
}
