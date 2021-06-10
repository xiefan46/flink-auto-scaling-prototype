package org.apache.flink.asc.datapipeline.dataprovider;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.asc.config.Config;
import com.linkedin.asc.datapipeline.DataPipeline;
import com.linkedin.asc.datapipeline.dataprovider.DiagnosticsStreamDataProvider;
import com.linkedin.asc.model.DiagnosticsMessage;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobSize;
import com.linkedin.asc.model.JobState;
import com.linkedin.asc.model.MetricHeader;
import com.linkedin.asc.model.MetricsSnapshot;
import com.linkedin.asc.model.TimeWindow;
import com.linkedin.asc.model.functions.FoldLeftFunction;
import com.linkedin.asc.model.functions.MaxFunc;
import com.linkedin.asc.store.Entry;
import com.linkedin.asc.store.KeyValueIterator;
import com.linkedin.asc.store.KeyValueStore;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.diagnostics.model.FlinkMetricsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlinkDiagnosticsStreamDataProvider implements DiagnosticsStreamDataProvider {

  private final static Logger LOG = LoggerFactory.getLogger(
      DiagnosticsStreamDataProvider.class.getName());


  protected final static int MAX_ATTEMPIDS_PER_JOB_TO_STORE = 100;

  private static final TemporalUnit TIMESTAMP_GRANULARITY = ChronoUnit.MINUTES;

  private static final String VCORE_USAGE_RATE_KEY_NAME = "Load";

  /**
   * Persistent states that store all the information we need for auto scaling
   */
  // KV store for storing sizing-related and other params used for auto scaling
  private final KeyValueStore<JobKey, JobState> jobStateStore;
  // KV store for storing job attempts, currently we only store the last MAX_ATTEMPIDS_PER_JOB_IN_JOBATTEMPTS_STORE attempts for a given jobName-instanceID
  private final KeyValueStore<String, LinkedList<String>> jobAttemptsStore;
  // KV store for cpu related metrics
  private final KeyValueStore<JobKey, TimeWindow> processVcoreUsageMetricStore;

  private final Duration processVCoreUsageWindowSize;

  public FlinkDiagnosticsStreamDataProvider(KeyValueStore<JobKey, JobState> jobStateStore,
      KeyValueStore<String, LinkedList<String>> jobAttemptsStore,
      KeyValueStore<JobKey, TimeWindow> processVcoreUsageMetricStore, Duration processVCoreUsageWindowSize) {
    this.jobStateStore = jobStateStore;
    this.jobAttemptsStore = jobAttemptsStore;
    this.processVcoreUsageMetricStore = processVcoreUsageMetricStore;
    this.processVCoreUsageWindowSize = processVCoreUsageWindowSize;
  }

  @Override
  public void receiveData(DiagnosticsMessage diagnosticsMessage, DataPipeline dataPipeline) {
    if(diagnosticsMessage != null) {
      boolean storeUpdated = this.updateJobStateAndAttemptStores(diagnosticsMessage, dataPipeline, diagnosticsMessage.getTimestamp());
      // if any jobState or attempt store was updated, only then we update the diagnostics/error stores, and cleanupIf reqd
      if (storeUpdated) {
        this.updateMetricWindowStores(diagnosticsMessage, diagnosticsMessage.getTimestamp());
      }
    }
  }

  /**
   * Get the latest attempts for each job known to the data-pipeline.
   * Each attempt is uniquely identified by a JobKey.
   * @return
   */
  @Override
  public Set<JobKey> getLatestAttempts() {
    Set<JobKey> latestJobAttempts = new HashSet<>();
    KeyValueIterator<String, LinkedList<String>> jobsIterator = this.jobAttemptsStore.all();
    while (jobsIterator.hasNext()) {
      Entry<String, LinkedList<String>> entry = jobsIterator.next();
      String jobId = entry.getKey();
      String latestAttemptID = entry.getValue().getLast();
      latestJobAttempts.add(
          new JobKey(jobId, latestAttemptID));
    }
    jobsIterator.close();
    return latestJobAttempts;
  }

  @Override
  public TimeWindow getProcessVcoreUsageMetricWindow(JobKey job) {
    return this.processVcoreUsageMetricStore.get(job);
  }

  /**
   * Get the recorded {@link JobState} for all jobs, instances, attempts.
   */
  public Map<JobKey, JobState> getAllJobsState() {
    Map<JobKey, JobState> jobsMap = new HashMap<>();
    KeyValueIterator<JobKey, JobState> jobsIterator = this.jobStateStore.all();
    jobsIterator.forEachRemaining(entry -> jobsMap.put(entry.getKey(), entry.getValue()));
    jobsIterator.close();
    return jobsMap;
  }


  /**
   * jobAttemptsStore appropriately.
   * @return true if any store is updated, false otherwise
   */
  private boolean updateJobStateAndAttemptStores(DiagnosticsMessage diagnosticsMessage, DataPipeline dataPipeline, long eventTime) {
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

    if (isStaleAttempt(attemptID)) {
      LOG.info("AttemptID from DiagnosticsMessage: {} is stale, skipping its processing", diagnosticsMessage);
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
    if (jobState == null && diagnosticsMessage.isAutoScalingEnabled()) {

      Config jobConfig = dataPipeline.getJobConfig(jobKey);
      LOG.info("Creating new jobState for job: {}, eventTime: {},  config: {}", jobKey, eventTime, jobConfig);
      JobSize jobSize = getJobSize(jobConfig);
      jobState = new JobState(jobSize, eventTime);

      //TODO: Implement a lag monitor for lag policy
      //stopLagMonitoring(jobNameInstanceIDPair);
      //startLagMonitoring(jobNameInstanceIDPair, jobState);

    } else if (jobState == null && !diagnosticsMessage.isAutoScalingEnabled()) {

      LOG.debug("Cleaning up state for job: {}", jobID);
      deleteFromAllStores(jobID, this.jobAttemptsStore.get(jobID));
      //TODO: Add this when we have lag monitor
      //stopLagMonitoring(jobNameInstanceIDPair);
      return false;
    }

    if (jobState != null && jobState.getLastTime() <= eventTime) {

      // update the last time
      jobState.setLastTime(eventTime);

      // after validating the timestamp of the message, we also update the jobAttempts store if the attempt is not already present
      updateJobAttemptsStore(jobKey);

      // write the updated value to the store after updating the required params
      jobStateStore.put(jobKey, jobState);
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

  //TODO: Get the JobSize from job config
  private JobSize getJobSize(Config jobConfig) {
    return null;
  }

  /**
   * Update the jobAttempts store, if the current attempt has not already been seen.
   * We bound the list of attemptIDs stored to MAX_ATTEMPIDS_PER_JOB_IN_JOBATTEMPTS_STORE
   * @param jobKey
   */
  private void updateJobAttemptsStore(JobKey jobKey) {

    String jobID = jobKey.getJobId();
    LinkedList<String> attemptIDs = this.jobAttemptsStore.get(jobID);

    if (attemptIDs == null) {
      attemptIDs = new LinkedList<>();
      attemptIDs.add(jobKey.getAttemptId());
    } else if (!attemptIDs.contains(jobKey.getAttemptId())) {
      // need to check to handle duplicates (atleast once), because there could be a failure after
      // updating jobAttemptsStore but before updating jobStateStore below
      attemptIDs.add(jobKey.getAttemptId());
    } else {
      LOG.debug("AttemptID: {} for job: {} already known", jobKey.getAttemptId(), jobKey);
    }

    // if the size of the attempt-list exceeds max size, we remove oldest attempts
    List<String> attemptIDsToDelete = new ArrayList<>();
    while (attemptIDs.size() > MAX_ATTEMPIDS_PER_JOB_TO_STORE) {
      attemptIDsToDelete.add(attemptIDs.removeFirst());
    }

    this.jobAttemptsStore.put(jobID, attemptIDs);
    deleteFromAllStores(jobID, attemptIDsToDelete);
  }



  // Delete give jobID and attemptIDs from all stores
  private void deleteFromAllStores(String jobId, List<String> attemptIDsToDelete) {

    // if the list is empty we skip the store-get
    if (attemptIDsToDelete == null || attemptIDsToDelete.isEmpty()) {
      return;
    }

    LinkedList<String> attemptIDs = new LinkedList<>(this.jobAttemptsStore.get(jobId));
    attemptIDs.removeAll(attemptIDsToDelete);
    if (attemptIDs.isEmpty()) {
      this.jobAttemptsStore.delete(jobId);
    } else {
      this.jobAttemptsStore.put(jobId, attemptIDs);
    }

    // populate the jobKeysToDelete list
    List<JobKey> jobKeysToDelete = new ArrayList<>();
    attemptIDsToDelete.forEach(attemptID -> jobKeysToDelete.add(new JobKey(jobId, attemptID)));

    if (!attemptIDsToDelete.isEmpty()) {
      LOG.info("Cleaning stores. jobKeysToDelete: {} attemptIDsToDelete: {}", jobKeysToDelete, attemptIDsToDelete);
    }

    this.jobStateStore.deleteAll(jobKeysToDelete);

    // Delete jobKey from metric stores
    this.processVcoreUsageMetricStore.deleteAll(jobKeysToDelete);
  }

  /**
   * Extract the required memory metrics from the snapshot and add to the corresponding {@link TimeWindow}.
   * Because of Samza's atleast once semantics, a snapshot may be delivered more than once. The TimeWindow
   * currently computes a max() across values which is an idempotent operation.
   * // TODO: if metric-window uses non idempotent ops, e.g., SUM, Avg, then discard based on eventTime
   *
   * @param diagnosticsMessage the metrics message to process
   * @param eventTime the event time associated with the snapshot
   */
  private void updateMetricWindowStores(DiagnosticsMessage diagnosticsMessage, long eventTime) {

    // Job Key uniquely identifies each attempt and instance of each job
    MetricHeader metricsHeader = diagnosticsMessage.getMetricHeader();
    String containerName = metricsHeader.getContainerName();

    JobKey jobKey = new JobKey(metricsHeader.getJobId(), metricsHeader.getJobAttemptId());
    Instant timestamp = Instant.ofEpochMilli(eventTime);

    // TODO: Check if we need to exclude JM metrics for Flink

    double processVcoreUsage = extractProcessVcoreUsage(diagnosticsMessage);
    addMetricValueToStore(processVcoreUsageMetricStore, jobKey, timestamp, processVcoreUsage,
        processVCoreUsageWindowSize, MaxFunc.MAX_FUNC);
  }


  private double extractProcessVcoreUsage(DiagnosticsMessage diagnosticsMessage) {
    MetricsSnapshot metricsSnapshot = diagnosticsMessage.getMetricsSnapshot();
    Map<String, Object> cpuMetricGroup = FlinkMetricsSnapshot.visitMetricGroupMap(metricsSnapshot,
        new String[]{"taskmanager", "Status", "CPU"}, false);
    if(cpuMetricGroup == null) {
      LOG.error("Can't find cpuMetricGroup in diagnosticsMessage");
      return 0;
    }
    double vcoreUsageRate = new Double(String.valueOf(cpuMetricGroup.get(VCORE_USAGE_RATE_KEY_NAME)));
    return vcoreUsageRate;
  }

  private void addMetricValueToStore(KeyValueStore<JobKey, TimeWindow> store, JobKey jobKey, Instant timestamp,
      double metric, Duration metricWindowLength, FoldLeftFunction<Double, Double> aggregateFunc) {
    TimeWindow timeWindow = store.get(jobKey);
    if (timeWindow == null) {
      LOG.info("Adding job: {} to store", jobKey);
      timeWindow = new TimeWindow(metricWindowLength);
    }
    timeWindow.setLength(metricWindowLength);
    timeWindow.addMetric(timestamp, metric, aggregateFunc);
    store.put(jobKey, timeWindow);
  }
}
