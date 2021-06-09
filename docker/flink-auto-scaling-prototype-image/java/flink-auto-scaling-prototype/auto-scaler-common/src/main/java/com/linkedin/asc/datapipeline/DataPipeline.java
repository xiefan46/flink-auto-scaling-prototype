package com.linkedin.asc.datapipeline;



import com.linkedin.asc.config.Config;
import com.linkedin.asc.datapipeline.dataprovider.ConfigDataProvider;
import com.linkedin.asc.datapipeline.dataprovider.DiagnosticsStreamDataProvider;
import com.linkedin.asc.model.DiagnosticsMessage;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobState;
import com.linkedin.asc.model.TimeWindow;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Stores and buffers all incoming data and metrics from multiple data providers.
 * {@link DiagnosticsStreamDataProvider} - parses the diagnostics stream and buffers data.
 */
@AllArgsConstructor
public class DataPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DataPipeline.class);

  //Data providers
  private final DiagnosticsStreamDataProvider diagnosticsStreamDataProvider;

  private final ConfigDataProvider configDataProvider;


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
    return this.diagnosticsStreamDataProvider.getProcessVcoreUsageMetricWindow(job);
  }

  public Config getJobConfig(JobKey jobKey) {
    return this.configDataProvider.getJobConfig(jobKey);
  }

}
