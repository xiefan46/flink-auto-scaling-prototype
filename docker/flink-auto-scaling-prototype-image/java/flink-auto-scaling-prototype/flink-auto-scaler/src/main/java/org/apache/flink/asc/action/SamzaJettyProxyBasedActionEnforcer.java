package org.apache.flink.asc.action;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.samza.helper.ConfigSerdeHelper;
import com.linkedin.samza.job.AutosizingStreamJob;
import com.linkedin.samza.sizing.Fabric;
import com.linkedin.samzaasc.datapipeline.dataprovider.CoordinatorStreamConfigDataProvider;
import com.linkedin.samzaasc.model.JobSize;
import com.linkedin.samzaasc.model.SizingAction;
import com.linkedin.samzadiagnosticscore.datamodel.job.JobKey;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.JobRunner;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.Section;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.Type;


/**
 * {@link ActionEnforcer} implementation that applies a {@link SizingAction} by first writing the required
 * sizing parameters to the coordinator-stream, and then re-starting the job by sending the stop and start
 * requests to Samza-Jetty-Proxy.
 */
public class SamzaJettyProxyBasedActionEnforcer implements ActionEnforcer {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaJettyProxyBasedActionEnforcer.class.getName());

  // Fabric in which this service is running
  private final Fabric fabric;

  // Time to sleep between successive calls to jetty-proxy to check status of a job
  private static final Duration JOB_STATUS_CHECK_PERIOD = Duration.ofSeconds(1);

  // Config object that contains config values for all the systems in the systemForCoordinatorStream list
  private final Config ascConfig;

  // String DNS format for Samza-Jetty-Proxy
  private static final String SAMZA_JETTY_PROXY_DNS_FORMAT = "samza-jetty-proxy.%s.tag.%s.atd.%s.";

  // Config for samza jetty proxy hosts
  private static final String CONFIG_FOR_SAMZA_PROXY_HOSTS = "samza.proxy.hosts";

  // Config for Env-info-finder
  private static final String CONFIG_FOR_ENV_INFO_FINDER = "envInfoFinder.fabricNameToDnsZoneMap";

  // Samza-li config that define a job's target cluster pool
  private static final String CONFIG_FOR_CLUSTER_TAG = "samza.cluster.tag";

  // If the job's config had no tag, we use this default tag when determining the jetty-proxy host for deploying it
  private static final String DEFAULT_SAMZA_CLUSTER_TAG = "primary";

  private CoordinatorStreamConfigDataProvider coordinatorStreamConfigDataProvider;

  public SamzaJettyProxyBasedActionEnforcer(Fabric fabric, Config ascConfig) {
    this.fabric = fabric;
    this.ascConfig = ascConfig;
    this.coordinatorStreamConfigDataProvider = new CoordinatorStreamConfigDataProvider(ascConfig);
    LOG.info("Initialized in {}", fabric);
  }

  @Override
  public void applyAction(SizingAction sizingAction) throws Exception {
    doApply(sizingAction, false);
  }

  @Override
  public void applyActionAndDisableAutoSizing(SizingAction sizingAction) throws Exception {
    doApply(sizingAction, true);
  }

  private void doApply(SizingAction sizingAction, boolean disableAutoSizing) throws Exception {
    LOG.info("Beginning action application for {}", sizingAction);

    JobKey targetJob = sizingAction.getJobKey();

    // Create a system-consumer to read the coordinator stream of the target job
    // obtain the coordinator-stream system name from the asc's config

    SystemStream targetJobCoordStream = coordinatorStreamConfigDataProvider.getTargetJobCoordStream(targetJob);

    // Read the current config of the job from coordinator stream
    Config currentConfigOfJob =
        coordinatorStreamConfigDataProvider.readConfigFromCoordinatorStream(targetJobCoordStream);

    // Configs to realize the target job size
    Config resizeConfigsToAdd =
        getConfigsForSize(sizingAction.getTargetJobSize(), currentConfigOfJob, disableAutoSizing);
    LOG.info("Translated action: {} to additional configs: {}", sizingAction, resizeConfigsToAdd);

    // Configs for the jetty-proxy hosts for the job
    // This is because samza.proxy.hosts config in the job's config (or the asc's config) may be outdated since
    // the set of jetty-proxy hosts may have changed since deploy (e.g., failover, upgrades, etc)
    Config configForJettyProxyHosts = getConfigsForJettyProxyHosts(this.fabric, currentConfigOfJob);
    LOG.info("Adding additional configs: {} for job: {}", configForJettyProxyHosts, targetJob);

    // Obtain the new config of the job
    List<Map<String, String>> configs = new ArrayList<>();
    configs.add(currentConfigOfJob);
    configs.add(resizeConfigsToAdd);
    configs.add(configForJettyProxyHosts);
    MapConfig newConfigOfJob = new MapConfig(configs);
    LOG.info("New config of job: {}  {}", sizingAction.jobKey, newConfigOfJob);

    // Write the updated config back to the coordinator stream
    writeConfigToCoordinatorStream(targetJobCoordStream, newConfigOfJob);

    // Issue request to Samza-jetty-proxy to re-start the job
    restartJob(newConfigOfJob);

    LOG.info("Action application for {} complete", sizingAction);
  }

  /**
   * Issue requests to samza-jetty-proxy to kill current job and restart.
   * @param newConfigOfJob config of the target job
   */
  private void restartJob(Config newConfigOfJob) throws InterruptedException {
    AutosizingStreamJob job = new AutosizingStreamJob(newConfigOfJob);
    job.kill();
    job.submit();

    // Wait until job is in new (i.e. YARN-accepted) state
    while (!job.getStatus().getStatusCode().equals(ApplicationStatus.StatusCode.New)) {
      Thread.sleep(JOB_STATUS_CHECK_PERIOD.toMillis());
    }
  }

  /**
   * Get config params required to apply the given target job size.
   * @param targetJobSize the required job size. Notice that the targetJobSize can be null, which means that we only want
   *                      to disable auto-sizing for this job but do not want to do any resize action. Also notice that
   *                      All the config parameters inside JobSize need to be positive values
   * @param disableAutoSizing disable auto-sizing in the new deployment
   * @return the configs to realize the given job size.
   */
  @VisibleForTesting
  static Config getConfigsForSize(JobSize targetJobSize, Config currentConfig, boolean disableAutoSizing) {
    Map<String, String> configs = new HashMap<>();
    if (!disableAutoSizing && targetJobSize == null) {
      return new MapConfig(configs);
    }
    if (disableAutoSizing) {
      configs.put(JobConfig.JOB_AUTOSIZING_ENABLED, "false");
      if (targetJobSize != null) {
        //Resets the job size to a desired value
        addValueToConfigMapIfValid(configs, ClusterManagerConfig.CLUSTER_MANAGER_MEMORY_MB, targetJobSize.getContainerMb());
        addValueToConfigMapIfValid(configs, ClusterManagerConfig.CLUSTER_MANAGER_MAX_CORES,  targetJobSize.getContainerNumCores());
        addValueToConfigMapIfValid(configs, JobConfig.JOB_CONTAINER_COUNT, targetJobSize.getContainerCount());
        addValueToConfigMapIfValid(configs, JobConfig.JOB_CONTAINER_THREAD_POOL_SIZE, targetJobSize.getContainerThreadPoolSize());
        //Replace the -Xmx config in task.opts with our desired value
        if (targetJobSize.getContainerXmxMb() > 0) {
          Optional<String> oldJvmOpts = Optional.ofNullable(currentConfig.get(ShellCommandConfig.TASK_JVM_OPTS));
          Optional<String> newJvmOpts =
              resetContainerMaxHeapSizeConfigInTaskOpts(oldJvmOpts, targetJobSize.getContainerXmxMb());
          configs.put(ShellCommandConfig.TASK_JVM_OPTS, newJvmOpts.get());
        }
      }
    } else {
      //Resets job size to a desired value but keeps auto-sizing enabled.
      addValueToConfigMapIfValid(configs, JobConfig.JOB_AUTOSIZING_CONTAINER_MEMORY_MB, targetJobSize.getContainerMb());
      addValueToConfigMapIfValid(configs, JobConfig.JOB_AUTOSIZING_CONTAINER_MAX_CORES, targetJobSize.getContainerNumCores());
      addValueToConfigMapIfValid(configs, JobConfig.JOB_AUTOSIZING_CONTAINER_COUNT, targetJobSize.getContainerCount());
      addValueToConfigMapIfValid(configs, JobConfig.JOB_AUTOSIZING_CONTAINER_THREAD_POOL_SIZE, targetJobSize.getContainerThreadPoolSize());
      addValueToConfigMapIfValid(configs, JobConfig.JOB_AUTOSIZING_CONTAINER_MAX_HEAP_MB, targetJobSize.getContainerXmxMb());
      // TODO: When doing scale-down, alter both Xmx and Xms (if an Xms value is present), else respect Xms
    }
    return new MapConfig(configs);
  }

  /**
   * Replace the -Xmx config in task.opts with the desired value
   */
  @VisibleForTesting
  static Optional<String> resetContainerMaxHeapSizeConfigInTaskOpts(Optional<String> jvmOpts, int containerXmxMb) {
    String xmxSetting = "-Xmx" + containerXmxMb + "m";
    if (jvmOpts.isPresent() && jvmOpts.get().contains("-Xmx")) {
      jvmOpts = Optional.of(jvmOpts.get().replaceAll("-Xmx\\S+", xmxSetting));
    } else if (jvmOpts.isPresent()) {
      jvmOpts = Optional.of(jvmOpts.get().concat(" " + xmxSetting));
    } else {
      jvmOpts = Optional.of(xmxSetting);
    }
    return jvmOpts;
  }

  //Add the config to config map if the value is valid (all the resource configs needs to be positive value, except
  //container thread pool size can be 0)
  private static void addValueToConfigMapIfValid(Map<String, String> configMap, String key, int value) {
    if (value > 0 || (value == 0 && key.equals(JobConfig.JOB_AUTOSIZING_CONTAINER_THREAD_POOL_SIZE))) {
      configMap.put(key, String.valueOf(value));
    }
  }

  /**
   * Get config params for specifying jetty.proxy hosts
   * @param fabric fabric in which this job/controller exists
   * @param currentConfigOfJob config of the target job
   * @return
   */
  @VisibleForTesting
  public Config getConfigsForJettyProxyHosts(Fabric fabric, Config currentConfigOfJob) throws IOException {
    String samzaClusterTag = currentConfigOfJob.get(CONFIG_FOR_CLUSTER_TAG, DEFAULT_SAMZA_CLUSTER_TAG);
    String proxyHosts = String.join(",", getProxyHostnames(samzaClusterTag, fabric, getDnsZone(fabric)));
    Config configForJettyProxyHosts = new MapConfig(Collections.singletonMap(CONFIG_FOR_SAMZA_PROXY_HOSTS, proxyHosts));
    return configForJettyProxyHosts;
  }

  private void writeConfigToCoordinatorStream(SystemStream targetJobCoordStream, Config newConfig) {

    LOG.info("Creating CoordinatorStreamSystemProducer for stream: {}", targetJobCoordStream);

    SystemFactory systemFactory = CoordinatorStreamUtil.getCoordinatorSystemFactory(ascConfig);
    SystemAdmin systemAdmin = systemFactory.getAdmin(targetJobCoordStream.getSystem(), ascConfig);
    SystemProducer systemProducer =
        systemFactory.getProducer(targetJobCoordStream.getSystem(), ascConfig, new MetricsRegistryMap());
    CoordinatorStreamSystemProducer coordinatorStreamSystemProducer = null;

    try {
      coordinatorStreamSystemProducer =
          new CoordinatorStreamSystemProducer(targetJobCoordStream, systemProducer, systemAdmin);

      // write the new config
      coordinatorStreamSystemProducer.register(JobRunner.SOURCE());
      coordinatorStreamSystemProducer.start();
      coordinatorStreamSystemProducer.writeConfig(JobRunner.SOURCE(), newConfig);
    } finally {

      if (coordinatorStreamSystemProducer != null) {
        coordinatorStreamSystemProducer.stop();
      }
    }

    LOG.info("Finished writing config to stream: {}", targetJobCoordStream);
  }

  /**
   * Helper method to query DNS to get a list of proxy-hosts to connect to when stop/starting a job for resizing.
   * @param clusterTag the tag identifying the current cluster of the job (e.g., primary, tesla, etc).
   * @param fabric the fabric of the job (e.g., ei-ltx1).
   * @param dnsZone the DNS zone of the fabric
   * @return the list of jetty-proxy hostnames
   * @throws IOException
   */
  private List<String> getProxyHostnames(String clusterTag, Fabric fabric, String dnsZone) throws IOException {
    List<String> jettyProxyHostnames = new ArrayList<>();

    // Construct a DNS query and issue it
    String dnsName = String.format(SAMZA_JETTY_PROXY_DNS_FORMAT, clusterTag, fabric.getValue(), dnsZone);
    Message response = getDNSResponse(dnsName);

    // throw an exception if DNS query failed
    if (response.getRcode() != Rcode.NOERROR) {
      LOG.error("DNS Resolution resulted in error, response: {}", response);
      throw new IOException(Rcode.string(response.getRcode()));
    }

    // Parse DNS query response to get hostnames
    Record[] records = response.getSectionArray(Section.ANSWER);
    List<String> addresses = Arrays.stream(records).map(Record::rdataToString).collect(Collectors.toList());
    for (String addr : addresses) {
      jettyProxyHostnames.add(InetAddress.getByName(addr).getHostName());
    }

    return jettyProxyHostnames;
  }

  /**
   * Helper method to get a DNS response for a given DNS name.
   * @param dnsName
   * @return DNS response {@link Message}
   */
  @VisibleForTesting
  public Message getDNSResponse(String dnsName) throws IOException {
    Record record = Record.newRecord(new Name(dnsName), Type.A, DClass.IN);
    return new SimpleResolver().send(Message.newQuery(record));
  }

  /**
   * Helper method to extract DNS Zone from the current config of a job.
   * @param fabric the current fabric of the job
   * @return
   */
  private String getDnsZone(Fabric fabric) {
    String dnsZoneInfoStr = ascConfig.get(CONFIG_FOR_ENV_INFO_FINDER);
    LOG.info("Fabric: {}, dnsZoneInfoStr: {}", fabric, dnsZoneInfoStr);
    Map<String, String> dnsZoneInfoMap =
        (Map<String, String>) ConfigSerdeHelper.INSTANCE.deserializeValue(dnsZoneInfoStr);
    LOG.info("Fabric: {}, dnsZoneInfoStr: {}, dnsZoneInfoMap: {} ", fabric, dnsZoneInfoStr, dnsZoneInfoMap);
    return dnsZoneInfoMap.get(fabric.getValue());
  }
}
