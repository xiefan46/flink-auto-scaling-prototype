package org.apache.flink.asc.job;

import com.linkedin.asc.action.ActionEnforcer;
import com.linkedin.asc.action.ActionRegistry;
import com.linkedin.asc.action.StoreBasedActionRegistry;
import com.linkedin.asc.config.ASCConfig;
import com.linkedin.asc.datapipeline.DataPipeline;
import com.linkedin.asc.datapipeline.dataprovider.ConfigDataProvider;
import com.linkedin.asc.datapipeline.dataprovider.DiagnosticsStreamDataProvider;
import com.linkedin.asc.datapipeline.dataprovider.ResourceManagerDataProvider;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobState;
import com.linkedin.asc.model.SizingAction;
import com.linkedin.asc.model.TimeWindow;
import com.linkedin.asc.policy.CPUScaleDownPolicy;
import com.linkedin.asc.policy.CPUScaleUpPolicy;
import com.linkedin.asc.policy.Policy;
import com.linkedin.asc.policy.resizer.Resizer;
import com.linkedin.asc.policy.resizer.StatelessJobResizer;
import com.linkedin.asc.store.KeyValueStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.asc.action.MockFlinkActionEnforcer;
import org.apache.flink.asc.datapipeline.dataprovider.FlinkDiagnosticsStreamDataProvider;
import org.apache.flink.asc.datapipeline.dataprovider.MockFlinkConfigDataProvider;
import org.apache.flink.asc.datapipeline.dataprovider.MockFlinkResourceManagerDataProvider;
import org.apache.flink.asc.store.FlinkKeyValueStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.diagnostics.model.FlinkDiagnosticsMessage;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlinkWindowableTask
    extends KeyedProcessFunction<String, FlinkDiagnosticsMessage, FlinkDiagnosticsMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkWindowableTask.class);

  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

  // JobName blacklist on which no policy should be applied, regardless of the per-policy whitelisting.
  private Pattern blacklistForAllPolicies;

  // Data for all jobs
  private DataPipeline dataPipeline;

  // ActionRegistry to take sizing actions
  private ActionRegistry actionRegistry;

  // Priority list of policies
  private List<Policy> policyList = new ArrayList<>();

  @Override
  public void open(Configuration parameters) throws Exception {
    /**
     * Scheduled a thread to evaluate all the policies periodically
     */
    scheduledExecutorService.schedule(new Runnable() {
      @Override
      public void run() {
        applyPolicies();
      }
    }, 1, TimeUnit.MINUTES);
    //TODO: Figure out how to get ascConfig
    ASCConfig ascConfig = null;
    this.blacklistForAllPolicies = ascConfig.getBlacklistForAllPolicies();
    this.dataPipeline = createDataPipeline(getRuntimeContext(), ascConfig);
    this.actionRegistry = createActionRegistry(getRuntimeContext(), ascConfig);
    initializePolicies(ascConfig);
  }

  @Override
  public void processElement(FlinkDiagnosticsMessage diagnosticsMessage, Context context,
      Collector<FlinkDiagnosticsMessage> collector) throws Exception {
    this.dataPipeline.processReceivedData(diagnosticsMessage);
  }

  private void initializePolicies(ASCConfig ascConfig) {
    Resizer resizer = new StatelessJobResizer(ascConfig.getMaxContainerMbForStateless(), ascConfig.getMaxJobMemoryMbForStateless(),
        ascConfig.getMaxContainerNumVcoresPerContainerForStateless(), ascConfig.getMaxContainerNumVcoresPerJobForStateless());
    CPUScaleUpPolicy cpuScaleUpPolicy = new CPUScaleUpPolicy(resizer, ascConfig.getCPUScalingPolicyScaleUpTriggerFactor(),
        ascConfig.getCPUScalingPolicyScaleUpMarginFraction(), ascConfig.getMetricWindowSizeForCPUScaleUp());
    CPUScaleDownPolicy cpuScaleDownPolicy = new CPUScaleDownPolicy(resizer, ascConfig.getCPUScalingPolicyScaleDownTriggerFactor(),
        ascConfig.getCPUScalingPolicyScaleDownMarginFraction(), ascConfig.getMetricWindowSizeForCPUScaleDown());
    policyList.add(cpuScaleUpPolicy);
    policyList.add(cpuScaleDownPolicy);
  }

  private DataPipeline createDataPipeline(RuntimeContext runtimeContext, ASCConfig ascConfig) {
    DiagnosticsStreamDataProvider diagnosticsStreamDataProvider =
        createDiagnosticsDataProvider(runtimeContext, ascConfig);
    ConfigDataProvider configDataProvider = new MockFlinkConfigDataProvider();
    DataPipeline dataPipeline = new DataPipeline(diagnosticsStreamDataProvider, configDataProvider);
    return dataPipeline;
  }

  private DiagnosticsStreamDataProvider createDiagnosticsDataProvider(RuntimeContext runtimeContext,
      ASCConfig ascConfig) {

    // KV store for storing sizing-related and other params used for auto scaling
    MapStateDescriptor<JobKey, JobState> jobStateStoreDescriptor =
        new MapStateDescriptor<>("jobStateStore", // the state name
            JobKey.class, JobState.class);
    KeyValueStore<JobKey, JobState> jobStateStore =
        new FlinkKeyValueStore<>(runtimeContext.getMapState(jobStateStoreDescriptor));

    // KV store for storing job attempts
    MapStateDescriptor<String, LinkedList<String>> jobAttemptsStoreDescriptor =
        new MapStateDescriptor<>("jobAttemptsStore", // the state name
            TypeInformation.of(new TypeHint<String>() {
            }), TypeInformation.of(new TypeHint<LinkedList<String>>() {
        }));

    KeyValueStore<String, LinkedList<String>> jobAttemptsStore =
        new FlinkKeyValueStore<>(runtimeContext.getMapState(jobAttemptsStoreDescriptor));

    // KV store for cpu related metrics
    MapStateDescriptor<JobKey, TimeWindow> processVcoreUsageMetricStoreDescriptor =
        new MapStateDescriptor<>("processVcoreUsageMetricStore", // the state name
            JobKey.class, TimeWindow.class);
    KeyValueStore<JobKey, TimeWindow> processVcoreUsageMetricStore =
        new FlinkKeyValueStore<>(runtimeContext.getMapState(processVcoreUsageMetricStoreDescriptor));

    Duration cpuScaleUpWindowSize = ascConfig.getMetricWindowSizeForCPUScaleUp();
    Duration cpuScaleDownWindowSize = ascConfig.getMetricWindowSizeForCPUScaleDown();
    //The size of the window for which the process-cpu-usage metric is to be stored.
    Duration metricWindowSizeForProcessVcoreUsage = ObjectUtils.max(cpuScaleDownWindowSize, cpuScaleUpWindowSize);
    LOG.info("metricWindowSizeForProcessVcoreUsage will be the larger value between "
            + "cpuScaleUpWindowSize({} ms) and cpuScaleDownWindowSize({} ms)", cpuScaleUpWindowSize.toMillis(),
        cpuScaleDownWindowSize.toMillis());

    DiagnosticsStreamDataProvider diagnosticsStreamDataProvider =
        new FlinkDiagnosticsStreamDataProvider(jobStateStore, jobAttemptsStore, processVcoreUsageMetricStore,
            metricWindowSizeForProcessVcoreUsage);

    return diagnosticsStreamDataProvider;
  }

  private ActionRegistry createActionRegistry(RuntimeContext runtimeContext, ASCConfig ascConfig) {
    MapStateDescriptor<String, List<SizingAction>> pendingActionsStoreDescriptor =
        new MapStateDescriptor<>("pendingActionsStore",
            TypeInformation.of(new TypeHint<String>() {
            }), TypeInformation.of(new TypeHint<List<SizingAction>>() {
        }));
    KeyValueStore<String, List<SizingAction>> pendingActionsStore =
        new FlinkKeyValueStore<>(runtimeContext.getMapState(pendingActionsStoreDescriptor));
    ResourceManagerDataProvider resourceManagerDataProvider = new MockFlinkResourceManagerDataProvider();
    ActionEnforcer actionEnforcer = new MockFlinkActionEnforcer();
    ActionRegistry actionRegistry = new StoreBasedActionRegistry(pendingActionsStore, resourceManagerDataProvider, actionEnforcer, "");
    return actionRegistry;
  }

  private void applyPolicies() {
    Set<SizingAction> sizingActions = new HashSet<>();

    // iterate over jobs' latest attempts which do not have pending actions
    for (JobKey job : this.dataPipeline.getLatestAttempts()) {

      // check if job-name matches blacklist, if so, skip all policies on it
      if (blacklistForAllPolicies.matcher(job.getJobId()).matches()) {
        LOG.info("Skipping all policies over job: {}, because matches blacklist: {}", job, blacklistForAllPolicies);
        continue;
      }

      // Check if there is an action for the job already, if so we do not apply any policies on it
      // TODO: Modify this logic when job can be resized without redeploying, in that case, check if the action is
      //  pending/issued, and apply only if the action is in issued state. The policy needs to also check that the
      //  observations its acting on are after the application of the last issued action.
      if (!actionRegistry.hasAction(job)) {

        // iterate over policies in priority order
        for (Policy policy : policyList) {
          Optional<SizingAction> action = policy.apply(job, this.dataPipeline);
          if (action.isPresent()) {
            sizingActions.add(action.get());
            LOG.info("Policy: {} yielded sizing actions: {}", policy.getClass().getCanonicalName(), action);
            break;
          }
        }
      }
    }
  }
}

