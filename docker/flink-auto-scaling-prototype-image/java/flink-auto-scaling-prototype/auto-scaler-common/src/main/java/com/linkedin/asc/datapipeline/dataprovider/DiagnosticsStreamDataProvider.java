package com.linkedin.asc.datapipeline.dataprovider;

import com.linkedin.asc.model.DiagnosticsMessage;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobNameInstanceIDPair;
import com.linkedin.asc.model.JobState;
import com.linkedin.asc.model.MetricHeader;
import com.linkedin.asc.model.MetricsSnapshot;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
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

      boolean storeUpdated =
          this.updateJobStateAndAttemptStores(message, message.getTimestamp());
      // if any jobState or attempt store was updated, only then we update the diagnostics/error stores, and cleanupIf reqd
      if (storeUpdated) {
        // Job state restore info can only be updated after job-container mapping is updated in super.process().
        // Otherwise isJobRestoringState may be incorrect due to incorrect job-container mapping.
        this.updateJobStateRestorationInfo(metricsSnapshot);
        this.updateMetricWindowStores(metricsSnapshot, incomingMessageEnvelope.getEventTime());
      }

  }

  /**
   * jobAttemptsStore appropriately.
   * @return true if any store is updated, false otherwise
   */
  private boolean updateJobStateAndAttemptStores(DiagnosticsMessage diagnosticsMessage, long eventTime) {
    MetricHeader metricHeader = diagnosticsMessage.getMetricHeader();
    MetricsSnapshot metricsSnapshot = diagnosticsMessage.getMetricsSnapshot();
    String jobID = metricHeader.getJobId();
    String attemptID = metricHeader.getJobAttemptId();
    String containerName = metricHeader.getContainerName();
    JobKey jobKey = new JobKey(jobID, attemptID);


    if (StringUtils.isEmpty(attemptID)) {
      LOG.debug("AttemptID could not be extracted from DiagnosticsMessage: {}", diagnosticsMessage);
      return false;
    }

    if (isStaleAttempt(attemptID )) {
      LOG.info("AttemptID from metricsSnapshot: {} is stale, skipping its processing", metricsSnapshot.getAsMap());
      return false;
    }

    // We need to update the jobStateStore only
    // a. if this jobKey does not already exist,
    // Or b. if this message has a timestamp that is greater than the last-seen timestamp for this job key (event-time).
    // TODO: check if this need to be changed if we use exactly-once delivery in Flink
    // Note that, all containers will emit the job-sizing parameters such as num containers, container size, etc, but they do not
    // vary across containers. In this case, we retain the first job size we receive.

    // The first diagnostics-stream-message from a container will contain the auto-sizing setting (enabled or not)
    // if it doesn't we ignore this message
    JobState jobState = jobStateStore.get(jobKey);
    if (jobState == null && isAutoSizingEnabled(diagStreamMsg)) {


      int containerThreadPoolSize = diagStreamMsg.getContainerThreadPoolSize();
      LOG.info(
          "Creating new jobState for job: {}, inputs: {}, eventTime: {}, numTasks: {}, numStores: {}, container-thread-pool: {}, config: {}",
          jobKey, inputs, eventTime, getNumTasks(diagStreamMsg.getContainerModels()),
          diagStreamMsg.getNumPersistentStores(), containerThreadPoolSize, diagStreamMsg.getConfig());

      jobState = new JobState(diagStreamMsg.getNumPersistentStores(), getNumTasks(diagStreamMsg.getContainerModels()),
          diagStreamMsg.getContainerMb(), diagStreamMsg.getContainerNumCores(),
          diagStreamMsg.getContainerModels().size(), containerThreadPoolSize, inputs, eventTime);

      stopLagMonitoring(jobNameInstanceIDPair);
      startLagMonitoring(jobNameInstanceIDPair, jobState);
    } else if (jobState == null && !isAutoSizingEnabled(diagStreamMsg)) {

      LOG.debug("Cleaning up state for job: {} and stop lag monitoring", jobKey);
      stopLagMonitoring(jobNameInstanceIDPair);
      deleteFromAllStores(jobNameInstanceIDPair, this.jobAttemptsStore.get(jobNameInstanceIDPair));
      return false;
    }

    if (jobState != null && jobState.getLastTime() <= eventTime) {

      // update the last time
      jobState.setLastTime(eventTime);

      // Note: maxHeapMemory size is emitted by both AM and the containers, we only use the value emitted by a non-AM
      // Take a min across containers' reported max-heap-size
      if (diagStreamMsg.getMaxHeapSize() != null && !containerName.equals(APPLICATION_MASTER_CONTAINER_NAME)
          && diagStreamMsg.getMaxHeapSize() < jobState.getJobSize().getContainerMaxHeapSizeBytes()) {
        LOG.info("Updated containerMaxHeapBytes for {} for job {} to {}", containerName, jobKey,
            diagStreamMsg.getMaxHeapSize());
        jobState.updateContainerMaxHeapBytes(diagStreamMsg.getMaxHeapSize());
      }

      // if the message contains processor-stop-events (emitted by the AM)
      if (diagStreamMsg.getProcessorStopEvents() != null) {
        jobState.addProcessorStopEvents(diagStreamMsg.getProcessorStopEvents());
      }

      //If a diag message contains a job's config, use remoteTableLookup to extract remote table specific
      //information and stores them in a map.
      if (diagStreamMsg.getConfig() != null && !diagStreamMsg.getConfig().isEmpty()
          && jobState.getRemoteIOServicesInfoMap() == null) {
        LOG.info("Job: {} try to create table info map by using configs from diag stream message. Config : {}", jobKey,
            diagStreamMsg.getConfig());
        jobState = RemoteIODataProvider.updateRemoteIOInfoInJobState(jobKey, jobState, diagStreamMsg.getConfig());
      }

      /*
       If the remote-table-info window has not expired and information about the job using remote table is not known.
       Then we may need to update the remote table usage information
       */
      if (!remoteIODataProvider.hasRemoteTableInfoWindowExpired(jobState) && (jobState.getUsesRemoteTable() == null
          || !jobState.getUsesRemoteTable())) {
        Boolean isEmitted = RemoteIODataProvider.isJobEmitsRemoteTableMetrics(jobKey, metricsSnapshot);
        if (isEmitted != null) {
          LOG.info("job: {}, setting jobState.setUsesRemoteTable {}.", jobKey, isEmitted);
          jobState.setUsesRemoteTable(isEmitted);
        }
      } else if (remoteIODataProvider.hasRemoteTableInfoWindowExpired(jobState) && jobState.getUsesRemoteTable() == null) {
        // if remote table window has expired, and information about the job is unknown because
        // there was no table-api-config in the job's config, or there was no sensor registry metrics found in the
        // metrics snapshot upto the time of window expiry (e.g., job has no remoteio, or uses old samza version),
        // then we presume that the job does not use any remote table.
        LOG.info("job: {}, setting jobState.setUsesRemoteTable false.", jobKey);
        jobState.setUsesRemoteTable(false);
      }

      // after validating the timestamp of the message, we also update the jobAttempts store if the attempt is not already present
      updateJobAttemptsStore(jobKey);

      // write the updated value to the store after updating the required params
      jobStateStore.put(jobKey, jobState);
      this.metrics.incrementJobStateUpdateCount(jobNameInstanceIDPair);
      return true;
    } else if (jobState != null && jobState.getLastTime() > eventTime) {
      LOG.warn("Got message with eventTime " + eventTime + " < lasttime in job state store ");
    } else {
      LOG.debug("Message from job: {}, not added to jobStateStore, contains no job-state info", jobKey);
    }

    return false;
  }

  //TODO: Check stale deployment for Flink
  private boolean isStaleAttempt(String attemptID) {
    return false;
  }

  /**
   * Update the jobAttempts store, if the current attempt has not already been seen.
   * We bound the list of attemptIDs stored to MAX_ATTEMPIDS_PER_JOB_IN_JOBATTEMPTS_STORE
   * @param jobKey
   */
  private void updateJobAttemptsStore(JobKey jobKey) {

    JobNameInstanceIDPair jobNameInstanceIDPair =
        new JobNameInstanceIDPair(jobKey.getJobName(), jobKey.getInstanceID());
    LinkedList<String> attemptIDs = this.jobAttemptsStore.get(jobNameInstanceIDPair);

    if (attemptIDs == null) {
      attemptIDs = new LinkedList<>();
      attemptIDs.add(jobKey.getAttemptID());
    } else if (!attemptIDs.contains(jobKey.getAttemptID())) {
      // need to check to handle duplicates (atleast once), because there could be a failure after
      // updating jobAttemptsStore but before updating jobStateStore below
      attemptIDs.add(jobKey.getAttemptID());
    } else {
      LOG.debug("AttemptID: {} for job: {} already known", jobKey.getAttemptID(), jobKey);
    }

    // if the size of the attempt-list exceeds max size, we remove oldest attempts
    List<String> attemptIDsToDelete = new ArrayList<>();
    while (attemptIDs.size() > MAX_ATTEMPIDS_PER_JOB_TO_STORE) {
      attemptIDsToDelete.add(attemptIDs.removeFirst());
    }

    this.jobAttemptsStore.put(jobNameInstanceIDPair, attemptIDs);
    deleteFromAllStores(jobNameInstanceIDPair, attemptIDsToDelete);
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
