package org.apache.flink.asc.datapipeline.dataprovider;

import com.linkedin.asc.model.JobKey;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.flink.diagnostics.model.DiagnosticsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 */
public class DiagnosticsStreamDataProvider  {
  private final static Logger LOG = LoggerFactory.getLogger(DiagnosticsStreamDataProvider.class.getName());

  public final static String APPLICATION_MASTER_CONTAINER_NAME = "samza-container-ApplicationMaster";
  private final static String SAMZA_CONTAINER_START_UP_TIME_MS = "container-startup-time";
  private final static String SAMZA_CONTAINER_METRIC_PHYSICAL_MEMORY_MB = "physical-memory-mb";
  private final static String SAMZA_CONTAINER_THREAD_POOL_SIZE = "container-thread-pool-size";
  private final static String SAMZA_CONTAINER_ACTIVE_THREADS = "container-active-threads";
  private final static String JVM_METRIC_HEAP_COMMITTED_MB = "mem-heap-committed-mb";
  private final static String JVM_METRIC_HEAP_USED_MB = "mem-heap-used-mb";
  private final static String JVM_METRIC_PROCESS_CPU_USAGE = "process-cpu-usage";


  // blacklist to skip indexing of non-OOM errors
  private final static Pattern BLACKLIST_FOR_NON_JAVA_ERROR_TYPES = Pattern.compile("^(?!.*(java.lang.OutOfMemoryError)).*$");
  protected final static int MAX_ATTEMPIDS_PER_JOB_TO_STORE = 100;

  private static final TemporalUnit TIMESTAMP_GRANULARITY = ChronoUnit.MINUTES;


  public void receiveData(DiagnosticsMessage message) {

      /*boolean storeUpdated =
          this.updateJobStateAndAttemptStores(metricsSnapshot, incomingMessageEnvelope.getEventTime());
      // if any jobState or attempt store was updated, only then we update the diagnostics/error stores, and cleanupIf reqd
      if (storeUpdated) {
        super.process(metricsSnapshot, incomingMessageEnvelope.getEventTime());
        // Job state restore info can only be updated after job-container mapping is updated in super.process().
        // Otherwise isJobRestoringState may be incorrect due to incorrect job-container mapping.
        this.updateJobStateRestorationInfo(metricsSnapshot);
        this.updateMetricWindowStores(metricsSnapshot, incomingMessageEnvelope.getEventTime());
      }*/

  }

  /**
   * Get the latest attempts for each job known to the data-pipeline.
   * Each attempt is uniquely identified by a JobKey.
   * @return
   */
  public Set<JobKey> getLatestAttempts() {
    Set<JobKey> latestJobAttempts = new HashSet<>();
    /*Iterator<JobNameInstanceIDPair, LinkedList<String>> jobsIterator = null;
    while (jobsIterator.hasNext()) {
      Entry<JobNameInstanceIDPair, LinkedList<String>> entry = jobsIterator.next();
      JobNameInstanceIDPair jobNameInstanceIDPair = entry.getKey();
      String latestAttemptID = entry.getValue().getLast();
      latestJobAttempts.add(
          new JobKey(jobNameInstanceIDPair.getJobName(), jobNameInstanceIDPair.getInstanceID(), latestAttemptID));
    }
    jobsIterator.close();*/
    return latestJobAttempts;
  }


}
