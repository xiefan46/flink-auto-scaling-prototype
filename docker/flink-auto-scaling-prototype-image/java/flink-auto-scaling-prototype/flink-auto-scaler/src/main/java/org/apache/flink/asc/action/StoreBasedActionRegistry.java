package org.apache.flink.asc.action;

import com.linkedin.asc.action.ActionEnforcer;
import com.linkedin.asc.action.ActionRegistry;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobNameInstanceIDPair;
import com.linkedin.asc.model.SizingAction;
import com.linkedin.asc.store.KeyValueIterator;
import com.linkedin.asc.store.KeyValueStore;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.flink.asc.datapipeline.dataprovider.ResourceManagerDataProvider;
import org.apache.flink.asc.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Maintains a work-queue of pending-actions that is drained by providing the actions
 * to the action-enforcer asynchronously.
 */
@AllArgsConstructor
public class StoreBasedActionRegistry implements ActionRegistry {

  private final static Logger LOG = LoggerFactory.getLogger(StoreBasedActionRegistry.class.getName());

  // KVStore for queuing/storing pending actions, indexed by jobName-instanceID.
  // For each job, we store an ordered list of actions enqueued/issued for it.
  // We limit the size of the list so that its serialized size is less than 1 MB
  //private final KeyValueStore<JobNameInstanceIDPair, List<SizingAction>> pendingActionsStore;

  // Enforcer used to apply the registered action
  private ActionEnforcer actionEnforcer;

  // Executor service that drains the pendingActions queue
  private ExecutorService executorService;

  private final static int MAX_SIZING_ACTIONS_PER_JOB_TO_STORE = 2000;
  // this limit is based on the changelog per-message size limitation see {@link TestSizingActionSerde} for details.

  // Timeout for which the applyAction() runnable thread waits on its monitor, before timing out
  // this timeout serves for retries on pending-actions
  private static final Duration APPLY_ACTION_TIMEOUT = Duration.ofSeconds(5);
  private volatile boolean shouldShutdown = false;
  private volatile Duration shutdownWaitTimeout = Duration.ofSeconds(30);
  private final ResourceManagerDataProvider resourceManagerDataProvider;
  private final KeyValueStore<JobNameInstanceIDPair, List<SizingAction>> pendingActionsStore;

  /**
   * Initialize metric values using the contents of the store.
   */
  private void initializeMetricValues() {
    synchronized (pendingActionsStore) {

      // iterate on the pendingActionsStore and populate actionsToIssue
      KeyValueIterator<JobNameInstanceIDPair, List<SizingAction>> iterator = pendingActionsStore.all();
      while (iterator.hasNext()) {
        Map.Entry<JobNameInstanceIDPair, List<SizingAction>> actionList = iterator.next();
        long sizingActionsEnqueuedCount = actionList.getValue().stream().filter(action -> action.getStatus().equals(SizingAction.Status.ENQUEUED)).count();
        long sizingActionsIssuedCount = actionList.getValue().stream().filter(action -> action.getStatus().equals(SizingAction.Status.ISSUED)).count();
        LOG.info("Initializing metric job: {}, sizingActionsIssuedCount: {}", actionList.getKey(), sizingActionsIssuedCount);
        LOG.info("Initializing metric job: {}, sizingActionsEnqueuedCount: {}", actionList.getKey(), sizingActionsEnqueuedCount);
      }
      iterator.close();
    }
  }

  @Override
  public boolean hasAction(JobKey jobKey) {
    boolean hasAction = false;
    JobNameInstanceIDPair job = new JobNameInstanceIDPair(jobKey.getJobName(), jobKey.getInstanceID());

    // serialize all accesses of pendingActionsStore
    synchronized (pendingActionsStore) {

      List<SizingAction> actionList = this.pendingActionsStore.get(job);
      if (actionList != null) {

        // iterate over the list of actions for this job, to find one that was for this jobKey
        hasAction = !actionList.stream()
            .filter(action -> action.getJobKey().equals(jobKey))
            .collect(Collectors.toSet())
            .isEmpty();
      }
    }

    return hasAction;
  }

  @Override
  public void registerAction(SizingAction action) {
    LOG.info("Registering sizing action: {}", action);
    JobNameInstanceIDPair job =
        new JobNameInstanceIDPair(action.getJobKey().getJobName(), action.getJobKey().getInstanceID());

    // serialize all accesses of pendingActionsStore
    synchronized (pendingActionsStore) {
      List<SizingAction> jobActionList = pendingActionsStore.get(job);
      if (jobActionList == null) {
        // we use a linkedList so removeHead are O(1)
        jobActionList = new LinkedList<>();
      }
      jobActionList.add(action);
      trimActionList(jobActionList);
      this.pendingActionsStore.put(job, jobActionList);

      pendingActionsStore.notify();
    }
  }

  /**
   * Helper method to trim an actionList if it exceeds MAX_SIZING_ACTIONS_PER_JOB_TO_STORE
   * @param actionList action list to trim.
   */
  private void trimActionList(List<SizingAction> actionList) {
    while (actionList.size() > MAX_SIZING_ACTIONS_PER_JOB_TO_STORE) {
      SizingAction action = actionList.remove(0);
      LOG.info("Trimming action list, removing action: {}", action);
    }
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down");
    this.shouldShutdown = true;
    Utils.shutdownExecutor(executorService, shutdownWaitTimeout);
  }

  private class ApplyActionsRunnable implements Runnable {

    @Override
    public void run() {
      // we iterate over pendingActionsStore and filter out the sizing-actions that are in the enqueued state, store them
      // where the key is the action, and the value is the list from which the action came -- this allows easy updates after applying the action
      Map<SizingAction, List<SizingAction>> actionsToIssue = new HashMap<>();

      while (!shouldShutdown) {

        // serialize all accesses of pendingActionsStore
        synchronized (pendingActionsStore) {

          // wait till timeout expires or there is a notification
          try {
            pendingActionsStore.wait(APPLY_ACTION_TIMEOUT.toMillis());
          } catch (InterruptedException e) {
            LOG.info("InterruptedException in  ApplyActionsRunnable", e);
          }

          // iterate on the pendingActionsStore and populate actionsToIssue
          KeyValueIterator<JobNameInstanceIDPair, List<SizingAction>> iterator = pendingActionsStore.all();
          while (iterator.hasNext()) {
            Map.Entry<JobNameInstanceIDPair, List<SizingAction>> actionList = iterator.next();
            Optional<SizingAction> action = getActionToIssue(actionList.getValue());

            if (action.isPresent()) {
              LOG.info("Need to set size of job:{} to {}", actionList.getKey(), action.get().getTargetJobSize());
              actionsToIssue.put(action.get(), actionList.getValue());
            }
          }
          iterator.close();
        }

        // iterate over all actionsToIssue
        for (Map.Entry<SizingAction, List<SizingAction>> actionEntry : actionsToIssue.entrySet()) {
          try {
            SizingAction action = actionEntry.getKey();
            boolean resourcesAvailable =
                resourceManagerDataProvider.verifyCapacity(action.getJobKey(), action.getCurrentJobSize(),
                    action.getTargetJobSize());

            // if resources-requires for this action are not available, skip on this action
            // TODO : Emit metrics/analytics https://jira01.corp.linkedin.com:8443/browse/LISAMZA-17736
            if (!resourcesAvailable) {
              LOG.error("Skipping action: {}, resources required not available", action);
              continue;
            }

            // TODO: apply action based on if the job's current attemptID matches the one in action
            actionEnforcer.applyAction(action);

            // serialize all accesses of pendingActionsStore
            synchronized (pendingActionsStore) {
              // update the status
              action.setStatus(SizingAction.Status.ISSUED);

              JobNameInstanceIDPair job =
                  new JobNameInstanceIDPair(action.getJobKey().getJobName(), action.getJobKey().getInstanceID());

              // persist the update
              pendingActionsStore.put(job, actionEntry.getValue());

            }
          } catch (Exception e) {
            LOG.info("Exception while applying action", e);
          }
        }

        // clear the actionsToIssue map for the next round
        actionsToIssue.clear();
      }
    }

    // Helper method that returns the first action from the list that has ENQUEUED status
    // TODO: make this method smarter by performing the last-action on a job if there are multiple actions
    // in enqueued actions for one jobName-instanceID
    private Optional<SizingAction> getActionToIssue(List<SizingAction> actionList) {
      for (SizingAction action : actionList) {
        if (action.getStatus().equals(SizingAction.Status.ENQUEUED)) {
          return Optional.of(action);
        }
      }

      return Optional.empty();
    }
  }
      }
    }
  }
}
