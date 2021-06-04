package com.linkedin.asc.datapipeline.dataprovider;

import com.linkedin.asc.model.DiagnosticsMessage;
import com.linkedin.asc.model.JobKey;
import java.util.Set;


/**
 * {@link DiagnosticsStreamDataProvider} receives {@link DiagnosticsMessage} and uses them to
 *  update {@link com.linkedin.asc.model.JobState} and consume qps info of a streaming job
 */
public interface DiagnosticsStreamDataProvider {

  void receiveData(DiagnosticsMessage diagnosticsMessage);

  Set<JobKey> getLatestAttempts();
}
