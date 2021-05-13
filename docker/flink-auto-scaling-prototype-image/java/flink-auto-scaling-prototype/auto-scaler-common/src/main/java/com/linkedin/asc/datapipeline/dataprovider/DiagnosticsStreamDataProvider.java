package com.linkedin.asc.datapipeline.dataprovider;

import com.linkedin.asc.datapipeline.DataPipeline;
import com.linkedin.asc.model.DiagnosticsMessage;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobState;
import com.linkedin.asc.model.TimeWindow;
import java.util.Map;
import java.util.Set;


/**
 * {@link DiagnosticsStreamDataProvider} receives {@link DiagnosticsMessage} and uses them to
 *  update {@link JobState} and consume qps info of a streaming job
 */
public interface DiagnosticsStreamDataProvider {

  void receiveData(DiagnosticsMessage diagnosticsMessage, DataPipeline dataPipeline);

  Set<JobKey> getLatestAttempts();

  TimeWindow getProcessVcoreUsageMetricWindow(JobKey job);

  Map<JobKey, JobState> getAllJobsState();

  /**
   * Return the job state for the given job.
   */
  JobState getJobState(JobKey jobKey);
}
