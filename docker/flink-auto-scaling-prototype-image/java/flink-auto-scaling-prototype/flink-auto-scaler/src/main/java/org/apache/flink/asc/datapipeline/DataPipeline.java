package org.apache.flink.asc.datapipeline;



import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobState;
import com.linkedin.asc.model.TimeWindow;
import java.util.Set;
import org.apache.flink.asc.datapipeline.dataprovider.DiagnosticsStreamDataProvider;
import org.apache.flink.diagnostics.model.DiagnosticsMessage;
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
