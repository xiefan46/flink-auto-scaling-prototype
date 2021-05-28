package org.apache.flink.asc.datapipeline;


import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.flink.asc.datapipeline.dataprovider.DiagnosticsStreamDataProvider;
import org.apache.flink.asc.model.JobKey;
import org.apache.flink.asc.model.JobState;
import org.apache.flink.asc.model.TimeWindow;
import org.apache.flink.diagnostics.model.DiagnosticsMessage;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Stores and buffers all incoming data and metrics from multiple data providers.
 * {@link DiagnosticsStreamDataProvider} - parses the diagnostics stream and buffers data.
 */

public class DataPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DataPipeline.class);

  //Data providers
  private DiagnosticsStreamDataProvider diagnosticsStreamDataProvider;


  public void processReceivedData(DiagnosticsMessage diagnosticsMessage) {
    this.diagnosticsStreamDataProvider.receiveData(diagnosticsMessage);
  }

  /**
   * Get the latest attempts for each job known to the data-pipeline.
   * Each attempt is uniquely identified by a JobKey.
   * @return
   */
  public Set<JobKey> getLatestAttempts() {
    return this.diagnosticsStreamDataProvider.getLatestAttempts();
  }

  public JobState getCurrentJobState(JobKey job) {
    return null;
  }

  public TimeWindow getProcessVcoreUsageMetricWindow(JobKey job) {
    return null;
  }

}
