package com.linkedin.asc.action;

import com.linkedin.asc.datapipeline.dataprovider.ResourceManagerDataProvider;
import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.JobSize;
import com.linkedin.asc.model.SizingAction;
import com.linkedin.asc.store.InternalInMemoryStore;
import com.linkedin.asc.store.KeyValueStore;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStoreBasedActionRegistry {

  private static final JobKey JOB_KEY = new JobKey("jobname",  "attempt ID 1");

  @Mock
  private ResourceManagerDataProvider resourceManagerDataProvider;
  @Mock
  private ActionEnforcer actionEnforcer;

  @BeforeClass
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testPendingActions() throws InterruptedException {
    CountDownLatch numberOfAnalyticsEventLatch = new CountDownLatch(2);
    StoreBasedActionRegistry actionRegistry = createNewRegistry(numberOfAnalyticsEventLatch);
    String jobId = JOB_KEY.getJobId();
    JobKey newJobKey = new JobKey(JOB_KEY.getJobId(), "attempt ID 2");
    Assert.assertFalse(actionRegistry.hasAction(JOB_KEY));

    Mockito.when(resourceManagerDataProvider.verifyCapacity(Mockito.eq(JOB_KEY), Mockito.any(JobSize.class),
        Mockito.any(JobSize.class))).thenReturn(true);
    Mockito.when(resourceManagerDataProvider.verifyCapacity(Mockito.eq(newJobKey), Mockito.any(JobSize.class),
        Mockito.any(JobSize.class))).thenReturn(true);
    actionRegistry.registerAction(
        new SizingAction(JOB_KEY, new JobSize(1000, 2, 2),
            new JobSize(1500, 2, 2), SizingAction.Type.JOB_TOTAL_MEMORY_SCALEUP));
    Assert.assertTrue(actionRegistry.hasAction(JOB_KEY));
    Assert.assertFalse(actionRegistry.hasAction(newJobKey));


    // submit action for new jobKey
    actionRegistry.registerAction(new SizingAction(newJobKey,
        new JobSize(3000, 4, 4),
        new JobSize(3000, 4, 4), SizingAction.Type.JOB_THREAD_SCALEUP));
    Assert.assertTrue(actionRegistry.hasAction(JOB_KEY));
    Assert.assertTrue(actionRegistry.hasAction(newJobKey));



    // we wait until two analytics events have been emitted
    numberOfAnalyticsEventLatch.await(30, TimeUnit.SECONDS);

  }

  //Action should proceed even if it is a legacy-action that does not contain the currentSize field
  @Test
  public void testActionWithNoCurrentSize() throws InterruptedException {
    CountDownLatch numberOfAnalyticsEventLatch = new CountDownLatch(1);
    StoreBasedActionRegistry actionRegistry = createNewRegistry(numberOfAnalyticsEventLatch);
    String jobId = JOB_KEY.getJobId();
    JobSize jobSize = new JobSize(1000, 2, 2);
    Mockito.when(resourceManagerDataProvider.verifyCapacity(JOB_KEY, null, jobSize)).thenReturn(true);
    actionRegistry.registerAction(
        new SizingAction(JOB_KEY, null, jobSize, SizingAction.Type.JOB_TOTAL_MEMORY_SCALEUP));
    Assert.assertTrue(actionRegistry.hasAction(JOB_KEY));

    numberOfAnalyticsEventLatch.await(30, TimeUnit.SECONDS);
  }


  //Should avoid scale up when the cluster's resource utilization (i.e., verifyCapacity) returns false
  @Test
  public void testBusyCluster() throws InterruptedException {
    CountDownLatch numberOfAnalyticsEventLatch = new CountDownLatch(2);
    StoreBasedActionRegistry actionRegistry = createNewRegistry(numberOfAnalyticsEventLatch);
    JobSize jobSize = new JobSize(1000, 2, 2);
    Mockito.when(
        resourceManagerDataProvider.verifyCapacity(Mockito.eq(JOB_KEY), Mockito.eq(jobSize), Mockito.any(JobSize.class)))
        .thenReturn(false);
    actionRegistry.registerAction(
        new SizingAction(JOB_KEY, jobSize, jobSize, SizingAction.Type.JOB_TOTAL_MEMORY_SCALEUP));
    Assert.assertTrue(actionRegistry.hasAction(JOB_KEY));

    // Regardless of how long the actual timeout, this action should never be executed
    Thread.sleep(1000);
  }

  private StoreBasedActionRegistry createNewRegistry(CountDownLatch numberOfAnalyticsEventLatch) {
    KeyValueStore<String, List<SizingAction>> pendingActionsStore = new InternalInMemoryStore<>();
    return new StoreBasedActionRegistry(pendingActionsStore, resourceManagerDataProvider,
        actionEnforcer, "");
  }
}
