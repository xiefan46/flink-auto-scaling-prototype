package com.linkedin.asc.config;

import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;


public class ASCConfig {
  private final static String CONFIG_FOR_AUTOSIZING_COVERAGE = "asc.coverage";

  private final static String CONFIG_FOR_STATELESS_MAX_CONTAINER_MB = "asc.policy.stateless.maxcontainermb";
  private final static String CONFIG_FOR_STATELESS_MAX_CONTAINER_NUM_THREADS =
      "asc.policy.stateless.maxcontainernumthreads";
  private final static String CONFIG_FOR_STATELESS_MAX_JOB_MEMORY = "asc.policy.stateless.maxjobmemorymb";
  private final static String CONFIG_FOR_STATELESS_MAX_NUM_VCORES_PER_CONTAINER =
      "asc.policy.stateless.maxnumvcorespercontainer";
  private final static String CONFIG_FOR_STATELESS_MAX_NUM_VCORES_PER_JOB =
      "asc.policy.stateless.maxnumvcoresperjob";

  //  Max amount of time-difference between current-timestamp and a job's last-known-timestamp
  // for policies to be applied on it
  private final static String CONFIG_FOR_POLICY_MAX_THRESHOLD_MS = "asc.policy.max-staleness.ms";

  // the amount of time a given set of lag values should span for taking action
  private final static String CONFIG_FOR_LAG_WINDOW_MS = "asc.policy.lag.window.ms";

  // the min amount of time-spacing between consecutive lag values
  private final static String CONFIG_FOR_LAG_PERIOD_MS = "asc.policy.lag.period.ms";

  // the bucket length to use when aggregating lag-values per-bucket to evaluate increase/decrease
  private final static String CONFIG_FOR_LAG_BUCKET_MS = "asc.policy.lag.bucket.ms";

  private final static String CONFIG_FOR_PHYSICAL_MEMORY_METRIC_WINDOW = "asc.metric.physicalmemory.window.ms";
  private final static String CONFIG_FOR_HEAP_USED_METRIC_WINDOW = "asc.metric.heapused.window.ms";
  private final static String CONFIG_FOR_HEAP_COMMITTED_METRIC_WINDOW = "asc.metric.heapcommitted.window.ms";
  private final static String CONFIG_FOR_THREAD_POOL_UTILIZATION_METRIC_WINDOW = "asc.metric.threadpoolutilization.window.ms";
  private final static String CONFIG_FOR_EVENT_LOOP_UTILIZATION_METRIC_WINDOW = "asc.metric.eventlooputilization.window.ms";

  private final static String CONFIG_FOR_XMX_MARGIN_FRACTION = "asc.policy.xmx.margin.fraction";
  private final static String CONFIG_FOR_XMX_MARGIN_MB = "asc.policy.xmx.margin.mb";

  private final static String CONFIG_FOR_CONTAINER_MEMORY_MB_MARGIN_FRACTION =
      "asc.policy.containermemorymb.margin.fraction";
  private final static String CONFIG_FOR_CONTAINER_MEMORY_MB_MARGIN_MB = "asc.policy.containermemorymb.margin.mb";

  //ResourceManagerDataProvider related configs
  private static final String CONFIG_FOR_RM_HOSTS = "resourcemanager.hosts";

  //A safeguard to avoid cpu scale up when the vcore util in the cluster is higher than this config.
  private static final String CONFIG_FOR_RM_MAX_VCORE_UTIL = "resourcemanager.max.vcore.util";
  private static final double DEFAULT_RM_MAX_VCORE_UTIL = 0.95;

  //A safeguard to avoid memory scale up when the memory util in the cluster is higher than this config.
  private static final String CONFIG_FOR_RM_MAX_MEMORY_UTIL = "resourcemanager.max.memory.util";
  private static final double DEFAULT_RM_MAX_MEMORY_UTIL = 0.95;

  // Blacklist of jobNames or jobName-instance on which no policy should be applied, regardless of the per-policy whitelisting.
  private final static String CONFIG_FOR_JOBNAME_BLACKLIST = "asc.policy.blacklist";
  private final static String DEFAULT_JOBNAME_BLACKLIST = "";

  // policy whitelist configs
  private final static String DEFAULT_POLICY_WHITELIST = ".*";
  private final static String CONFIG_FOR_MEMORYSCALEUP_POLICY = "asc.policy.memoryscaleup.whitelist";
  private final static String CONFIG_FOR_LAGBASEDTHREADSCALEUP_POLICY = "asc.policy.lag.whitelist";
  private final static String CONFIG_FOR_THREADSCALEDOWN_POLICY = "asc.policy.threadscaledown.whitelist";
  private final static String CONFIG_FOR_LAGBASEDTHREADSCALEUP_REMOTE_TABLE_POLICY = "asc.policy.lag.remote-table.whitelist";
  private final static String CONFIG_FOR_HEAPSCALEDOWN_POLICY = "asc.policy.heapscaledown.whitelist";
  private final static String CONFIG_FOR_TOTALMEMORYSCALEDOWN_POLICY = "asc.policy.totalmemoryscaledown.whitelist";
  private final static String CONFIG_FOR_CPU_SCALEUP_POLICY = "asc.policy.cpuscaleup.whitelist";
  private final static String CONFIG_FOR_CPU_SCALEDOWN_POLICY = "asc.policy.cpuscaledown.whitelist";
  private final static String CONFIG_FOR_DISABLE_SIZING_STATE_RESTORING_STATEFUL_JOBS =
      "asc.policy.disable-sizing-state-restoring-stateful-jobs.whitelist";
  private final static String CONFIG_FOR_BOUND_JOB_SIZE_WITH_STATE_RESTORATION_INFO =
      "asc.policy.bound-job-size-with-state-restoration-info.whitelist";

  //Parameters for state restoration stage scaling
  private final static String CONFIG_FOR_STATE_RESTORATION_METRIC_WINDOW = "asc.policy.state-restoration.window.count";
  private final static String CONFIG_FOR_MIN_VALID_STATE_RESTORE_DURATION = "asc.policy.state-restoration.min.duration.ms";

  //Parameters for CPU scale up policy
  private final static String CONFIG_FOR_CPU_SCALEUP_WINDOW_SIZE = "asc.policy.cpuscaleup.window.ms";
  private final static String CONFIG_FOR_CPU_SCLING_POLICY_SCALE_UP_TRIGGER_FACTOR =
      "asc.policy.cpuscaleup.trigger.factor";
  private final static String CONFIG_FOR_CPU_SCLING_POLICY_SCALE_UP_MARGIN_FRACTION =
      "asc.policy.cpuscaleup.margin.fraction";

  //Parameters for CPU scale down policy
  private final static String CONFIG_FOR_CPU_SCALEDOWN_WINDOW_SIZE = "asc.policy.cpuscaledown.window.ms";
  private final static String CONFIG_FOR_CPU_SCLING_POLICY_SCALE_DOWN_TRIGGER_FACTOR =
      "asc.policy.cpuscaledown.trigger.factor";
  private final static String CONFIG_FOR_CPU_SCLING_POLICY_SCALE_DOWN_MARGIN_FRACTION =
      "asc.policy.cpuscaledown.margin.fraction";

  // Parameters for thread scale down policy
  private final static String CONFIG_FOR_THREAD_SCALE_DOWN_TRIGGER_FACTOR =
      "asc.policy.threadscaledown.trigger.factor";
  private final static String CONFIG_FOR_HIGH_EVENT_LOOP_UTILIZATION_THRESHOLD =
      "asc.policy.threadscaledown.high.event-loop-utilization.threshold";
  private final static String CONFIG_FOR_THREAD_SCALE_DOWN_MARGIN_FRACTION =
      "asc.policy.threadscaledown.margin.fraction";

  //Metrics whitelist config
  private final static String CONFIG_FOR_PROCESS_VCORE_USAGE_METRIC_WHITELIST =
      "asc.metric.process.vcore.usage.whitelist";

  // system to use when locating a job's coordinator-stream, unless any per-job override is not specified.

  private static final String CONFIG_FOR_COORDINATOR_STREAM_SYSTEMS = "asc.action.coordinator.stream.systems";
  private static final String CONFIG_FOR_COORDINATOR_STREAM_SYSTEMS_FOR_JOB =
      "asc.action.job.%s.%s.coordinator.stream.systems";

  // system and stream for pinot ingestion system and stream for analytics
  private static final String CONFIG_FOR_ANALYTICS_SYSTEM = "asc.analytics.pinot.ingest.systems";
  private static final String CONFIG_FOR_ANALYTICS_STREAM = "asc.analytics.pinot.ingest.stream";

  // config for remote-io
  private static final String CONFIG_FOR_REMOTE_IO_MAX_READ_RATE_LIMIT = "asc.policy.remoteio.max.read.rate.limit";
  private static final String CONFIG_FOR_REMOTE_IO_MAX_WRITE_RATE_LIMIT =
      "asc.policy.remoteio.max.write.rate.limit";
  private static final String CONFIG_FOR_REMOTE_IO_READ_QPS_WINDOW_MS = "asc.policy.remoteio.read.qps.window.ms";
  private static final String CONFIG_FOR_REMOTE_IO_WRITE_QPS_WINDOW_MS = "asc.policy.remoteio.write.qps.window.ms";
  private static final String CONFIG_FOR_REMOTE_IO_SCALEUP_TRIGGER_FACTOR =
      "asc.policy.remoteio.scaleup.trigger.factor";
  private static final String CONFIG_FOR_REMOTEIO_INFO_EXPIRED_WINDOW_MS =
      "asc.policy.remoteio.info.expired.window.ms";
  private static final String CONFIG_FOR_REMOTEIO_DEFAULT_PROVISION_LIMIT =
      "asc.policy.remoteio.default.provision.limit";


  private final Config config;

  public ASCConfig(Config config) {
    this.config = config;
  }

  /**
   * Coverage that decides which Samza jobs should be autosized.
   * Coverage should be all jobs except test jobs for i001, and test jobs only for i002 in any fabric. If i002 does not
   * exist, then i001 should autosize all jobs.
   */
  public Pattern getAutosizingCoverage() {
    return Pattern.compile((config.get(CONFIG_FOR_AUTOSIZING_COVERAGE)));
  }

  /**
   * Safeguard limit on the container memory mb size for stateless jobs.
   */
  public int getMaxContainerMbForStateless() {
    return Integer.valueOf(config.get(CONFIG_FOR_STATELESS_MAX_CONTAINER_MB));
  }

  /**
   * Safeguard limit on the container num threads for stateless jobs.
   * @return
   */
  public int getMaxContainerNumThreadsForStateless() {
    return Integer.valueOf(config.get(CONFIG_FOR_STATELESS_MAX_CONTAINER_NUM_THREADS));
  }

  /**
   * Safeguard limit on the container num vcores for stateless jobs.
   * @return
   */
  public int getMaxContainerNumVcoresPerContainerForStateless() {
    return Integer.valueOf(config.get(CONFIG_FOR_STATELESS_MAX_NUM_VCORES_PER_CONTAINER));
  }

  /**
   * Safeguard limit on the num vcores per job stateless jobs.
   * @return
   */
  public int getMaxContainerNumVcoresPerJobForStateless() {
    return Integer.valueOf(config.get(CONFIG_FOR_STATELESS_MAX_NUM_VCORES_PER_JOB));
  }

  /**
   * Safeguard limit on the container count for stateless jobs.
   * @return
   */
  public int getMaxJobMemoryMbForStateless() {
    return config.getInt(CONFIG_FOR_STATELESS_MAX_JOB_MEMORY);
  }

  /**
   * The window of lag values to consider for taking lag-based action.
   * @return
   */
  public Duration getLagWindowDuration() {
    return Duration.ofMillis(config.getLong(CONFIG_FOR_LAG_WINDOW_MS));
  }

  /**
   * The bucket size to use when bucketting lag-window to evaluate increase/decrease.
   * @return
   */
  public Duration getLagWindowBucketDuration() {
    Duration retVal = Duration.ofMillis(config.getLong(CONFIG_FOR_LAG_BUCKET_MS));
    if (retVal.compareTo(getLagWindowDuration()) > 0) {
      throw new ConfigException(
          "Lag bucket size: " + retVal + " is larger than lag window size: " + getLagWindowDuration());
    }

    return retVal;
  }

  /**
   * The min spacing between lag datapoints within the CONFIG_FOR_LAG_WINDOW_MS window.
   * @return
   */
  public Duration getLagPeriodDuration() {
    return Duration.ofMillis(config.getLong(CONFIG_FOR_LAG_PERIOD_MS));
  }

  /**
   * The size of the window for which the physical-memory-mb metric is to be stored.
   * @return
   */
  public Duration getMetricWindowSizeForPhysicalMemory() {
    return Duration.ofMillis(config.getLong(CONFIG_FOR_PHYSICAL_MEMORY_METRIC_WINDOW));
  }

  /**
   * The size of the window for which the heap-used metric is to be stored.
   * @return
   */
  public Duration getMetricWindowSizeForHeapUsed() {
    return Duration.ofMillis(config.getLong(CONFIG_FOR_HEAP_USED_METRIC_WINDOW));
  }

  /**
   * The size of the window for which the heap-committed metric is to be stored.
   * @return
   */
  public Duration getMetricWindowSizeForHeapCommitted() {
    return Duration.ofMillis(config.getLong(CONFIG_FOR_HEAP_COMMITTED_METRIC_WINDOW));
  }

  /**
   * Window size for CPUScaleUpPolicy
   * @return
   */
  public Duration getMetricWindowSizeForCPUScaleUp() {
    return Duration.ofMillis(config.getLong(CONFIG_FOR_CPU_SCALEUP_WINDOW_SIZE));
  }

  /**
   * Window size for CPUScaleDownPolicy
   * @return
   */
  public Duration getMetricWindowSizeForCPUScaleDown() {
    return Duration.ofMillis(config.getLong(CONFIG_FOR_CPU_SCALEDOWN_WINDOW_SIZE));
  }

  /**
   * The size of the window for which container thread pool utilization is to be stored.
   * @return
   */
  public Duration getMetricWindowSizeForThreadPoolUtilization() {
    return Duration.ofMillis(config.getLong(CONFIG_FOR_THREAD_POOL_UTILIZATION_METRIC_WINDOW));
  }

  /**
   * The size of the window for which container event loop utilization is to be stored.
   * @return
   */
  public Duration getMetricWindowSizeForEventLoopUtilization() {
    return Duration.ofMillis(config.getLong(CONFIG_FOR_EVENT_LOOP_UTILIZATION_METRIC_WINDOW));
  }

  /**
   * Only calculate resource usage for the time period that takes container-startup-ms from zero to non-zero if it is
   * longer than a certain threshold to eliminate the impact of container redeployment without state restore, e.g.
   * redeployment with host affinity enabled
   */
  public Duration getMinValidStateRestoreDuration() {
    return Duration.ofMillis(config.getLong(CONFIG_FOR_MIN_VALID_STATE_RESTORE_DURATION));
  }

  /**
   * The size of the window for which the state restoration related metrics (heap committed, physical memory, vcore
   * usage) are stored.
   * @return
   */
  public int getMetricWindowSizeForStateRestoration() {
    return config.getInt(CONFIG_FOR_STATE_RESTORATION_METRIC_WINDOW);
  }

  /**
   * The max allowable headroom between max-heap-committed and max-heap-configured (Xmx).
   * A value of 0.20 implies Xmx can be atmost 20% higher than max-heap-committed.
   */
  public double getXmxMarginFraction() {
    return config.getDouble(CONFIG_FOR_XMX_MARGIN_FRACTION);
  }

  /**
   * The max allowable headroom between max-heap-committed and max-heap-configured (Xmx) in MB.
   */
  public int getXmxMarginMb() {
    return config.getInt(CONFIG_FOR_XMX_MARGIN_MB);
  }

  /**
   * The max allowable headroom between max-physical-memory-mb/max-heap-committed and container-memory-mb.
   * A value of 0.20 implies container-memory-mb can be atmost 20% higher than max-physical-memory-mb/max-heap-committed.
   * @return
   */
  public double geContainerMemoryMarginFraction() {
    return config.getDouble(CONFIG_FOR_CONTAINER_MEMORY_MB_MARGIN_FRACTION);
  }

  /**
   * The max allowable headroom between max-physical-memory-mb/max-heap-committed and container-memory-mb in MB
   * @return
   */
  public int getContainerMemoryMarginMb() {
    return config.getInt(CONFIG_FOR_CONTAINER_MEMORY_MB_MARGIN_MB);
  }

  /**
   * Get the blacklist of jobNames on which no policy should be applied, regardless of the per-policy whitelisting.
   * @return
   */
  public Pattern getBlacklistForAllPolicies() {
    return Pattern.compile(config.get(CONFIG_FOR_JOBNAME_BLACKLIST, DEFAULT_JOBNAME_BLACKLIST));
  }

  /**
   * Get the whitelist to use for MemoryScaleupPolicy
   */
  public Pattern getWhitelistForMemoryScaleupPolicy() {
    return Pattern.compile(config.get(CONFIG_FOR_MEMORYSCALEUP_POLICY, DEFAULT_POLICY_WHITELIST));
  }

  /**
   * Get the whitelist to use for LagBasedThreadScaleupPolicy
   */
  public Pattern getWhitelistForLagBasedThreadScaleupPolicy() {
    return Pattern.compile(config.get(CONFIG_FOR_LAGBASEDTHREADSCALEUP_POLICY, DEFAULT_POLICY_WHITELIST));
  }

  /**
   * Get the whitelist to use for ThreadScaledownPolicy
   */
  public Pattern getWhitelistForThreadScaledownPolicy() {
    return Pattern.compile(config.get(CONFIG_FOR_THREADSCALEDOWN_POLICY, DEFAULT_POLICY_WHITELIST));
  }

  /**
   * Get the whitelist to use for LagBasedThreadScaleupPolicy even if jobs
   * use remote-table using table API.
   * @return
   */
  public Pattern getWhitelistForLagBasedPolicyForRemoteTableJobs() {
    return Pattern.compile(config.get(CONFIG_FOR_LAGBASEDTHREADSCALEUP_REMOTE_TABLE_POLICY));
  }

  /**
   * Get the whitelist to use for HeapScaledownPolicy
   */
  public Pattern getWhitelistForHeapScaledownPolicy() {
    return Pattern.compile(config.get(CONFIG_FOR_HEAPSCALEDOWN_POLICY, DEFAULT_POLICY_WHITELIST));
  }

  /**
   * Get the whitelist to use for TotalMemoryScaledownPolicy
   */
  public Pattern getWhitelistForTotalMemoryScaledownPolicy() {
    return Pattern.compile(config.get(CONFIG_FOR_TOTALMEMORYSCALEDOWN_POLICY, DEFAULT_POLICY_WHITELIST));
  }

  /**
   * Get the whitelist to use for CPUScaleUpPolicy
   */
  public Pattern getWhitelistForCPUScaleUpPolicy() {
    return Pattern.compile(config.get(CONFIG_FOR_CPU_SCALEUP_POLICY, DEFAULT_POLICY_WHITELIST));
  }

  /**
   * Get the whitelist to use for CPUScaleDownPolicy
   */
  public Pattern getWhitelistForCPUScaleDownPolicy() {
    return Pattern.compile(config.get(CONFIG_FOR_CPU_SCALEDOWN_POLICY, DEFAULT_POLICY_WHITELIST));
  }

  /**
   * List of jobs for which processVcoreUsed metric would be emitted
   */
  public Pattern getWhitelistForProcessVcoreUsageMetric() {
    return Pattern.compile(config.get(CONFIG_FOR_PROCESS_VCORE_USAGE_METRIC_WHITELIST));
  }

  /**
   * Get the whitelist to disable lag-based scale-up and all scale-down sizing actions during the state restore stage of
   * stateful jobs.
   */
  public Pattern getWhitelistForDisablingSizingForStateRestoringStatefulJobs() {
    return Pattern.compile(config.get(CONFIG_FOR_DISABLE_SIZING_STATE_RESTORING_STATEFUL_JOBS));
  }

  /**
   * Get the whitelist to bound the job size the metrics collected during the state restore stage of stateful jobs.
   */
  public Pattern getWhitelistForBoundJobSizeWithStateRestorationInfo() {
    return Pattern.compile(config.get(CONFIG_FOR_BOUND_JOB_SIZE_WITH_STATE_RESTORATION_INFO));
  }

  /**
   * Get the max amount of time-difference between current-timestamp and a job's last-known-timestamp,
   * for any policy to be applied on any job.
   *
   */
  public Duration getMaxStaleness() {
    return Duration.ofMillis(config.getLong(CONFIG_FOR_POLICY_MAX_THRESHOLD_MS));
  }

  /**
   * Get list of RM URI addresses
   */
  public List<String> getResourceManagerURIs() {
    return config.getList(CONFIG_FOR_RM_HOSTS);
  }

  /**
   * @return Scale up trigger factor for CPUScaleUpPolicy
   */
  public double getCPUScalingPolicyScaleUpTriggerFactor() {
    return config.getDouble(CONFIG_FOR_CPU_SCLING_POLICY_SCALE_UP_TRIGGER_FACTOR);
  }

  /**
   * @return Scale down trigger factor for CPUScaleDownPolicy
   */
  public double getCPUScalingPolicyScaleDownTriggerFactor() {
    return config.getDouble(CONFIG_FOR_CPU_SCLING_POLICY_SCALE_DOWN_TRIGGER_FACTOR);
  }

  /**
   * @return Scale up margin fraction for CPUScaleUpPolicy
   */
  public double getCPUScalingPolicyScaleUpMarginFraction() {
    return config.getDouble(CONFIG_FOR_CPU_SCLING_POLICY_SCALE_UP_MARGIN_FRACTION);
  }

  /**
   * @return Scale down margin fraction for CPUScaleDownPolicy
   */
  public double getCPUScalingPolicyScaleDownMarginFraction() {
    return config.getDouble(CONFIG_FOR_CPU_SCLING_POLICY_SCALE_DOWN_MARGIN_FRACTION);
  }

  /**
   * @return Scale down trigger factor for ThreadScaledownPolicy
   */
  public double getThreadScaledownTriggerFactor() {
    return config.getDouble(CONFIG_FOR_THREAD_SCALE_DOWN_TRIGGER_FACTOR);
  }

  /**
   * @return High event loop utilization threshold used in ThreadScaledownPolicy
   */
  public double getHighEventLoopUtilizationThreshold() {
    return config.getDouble(CONFIG_FOR_HIGH_EVENT_LOOP_UTILIZATION_THRESHOLD);
  }

  /**
   * @return Scale down margin fraction for ThreadScaledownPolicy
   */
  public double getThreadScaledownPolicyMarginFraction() {
    return config.getDouble(CONFIG_FOR_THREAD_SCALE_DOWN_MARGIN_FRACTION);
  }



  public double getRMMaxVcoreUtil() {
    return config.getDouble(CONFIG_FOR_RM_MAX_VCORE_UTIL, DEFAULT_RM_MAX_VCORE_UTIL);
  }

  public double getRMMaxMemoryUtil() {
    return config.getDouble(CONFIG_FOR_RM_MAX_MEMORY_UTIL, DEFAULT_RM_MAX_MEMORY_UTIL);
  }



  public double getRemoteIOScaleupTriggerFactor() {
    return config.getDouble(CONFIG_FOR_REMOTE_IO_SCALEUP_TRIGGER_FACTOR);
  }


  public long getRemoteIODefaultProvisionLimit() {
    return config.getLong(CONFIG_FOR_REMOTEIO_DEFAULT_PROVISION_LIMIT);
  }

}
