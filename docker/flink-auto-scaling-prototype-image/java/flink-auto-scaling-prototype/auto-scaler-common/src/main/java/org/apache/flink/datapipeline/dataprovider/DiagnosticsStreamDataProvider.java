package org.apache.flink.datapipeline.dataprovider;

import org.apache.flink.model.DiagnosticsMessage;
import org.apache.flink.model.JobKey;
import java.util.Set;
import org.apache.flink.model.JobState;


/**
 * {@link DiagnosticsStreamDataProvider} receives {@link DiagnosticsMessage} and uses them to
 *  update {@link JobState} and consume qps info of a streaming job
 */
public interface DiagnosticsStreamDataProvider {

  void receiveData(DiagnosticsMessage diagnosticsMessage);

  Set<JobKey> getLatestAttempts();
}
