package org.apache.flink.asc.job;

import org.apache.flink.datapipeline.DataPipeline;
import org.apache.flink.model.JobKey;
import org.apache.flink.model.SizingAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.flink.action.ActionRegistry;
import org.apache.flink.policy.Policy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.diagnostics.model.FlinkDiagnosticsMessage;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlinkWindowableTask extends KeyedProcessFunction<String, FlinkDiagnosticsMessage, FlinkDiagnosticsMessage> {

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
  }

  @Override
  public void processElement(FlinkDiagnosticsMessage diagnosticsMessage, Context context,
      Collector<FlinkDiagnosticsMessage> collector) throws Exception {
    this.dataPipeline.processReceivedData(diagnosticsMessage);
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

