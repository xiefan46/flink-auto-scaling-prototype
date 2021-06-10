package org.apache.flink.asc.store;

import com.linkedin.asc.model.JobKey;
import lombok.Getter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestFlinkKeyValueStore {
  private KeyedOneInputStreamOperatorTestHarness<String, JobKey, Integer> testHarness;
  private StatefulMapFunction statefulMapFunction;

  @Before
  public void setupTestHarness() throws Exception {

    //instantiate user-defined function
    statefulMapFunction = new StatefulMapFunction();

    // wrap user defined function into a the corresponding operator
    testHarness = new KeyedOneInputStreamOperatorTestHarness<>(new StreamMap<>(statefulMapFunction), JobKey::getJobId,
        TypeInformation.of(new TypeHint<String>() {
        }));

    // optionally configured the execution environment
    testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

    // open the test harness (will also call open() on RichFunctions)
    testHarness.open();
  }


  @Test
  public void testingFlinkKeyValueState() throws Exception {

    JobKey jobKey = new JobKey("test-job", "attempt-01");
    testHarness.processElement(jobKey, 100L);
    testHarness.processElement(jobKey, 101L);
    Assert.assertEquals(new Integer(2), statefulMapFunction.getKeyValueStore().get(jobKey));
  }

  class StatefulMapFunction extends RichMapFunction<JobKey, Integer> {

    @Getter
    FlinkKeyValueStore<JobKey, Integer> keyValueStore;

    @Override
    public void open(Configuration config) {
      MapStateDescriptor<JobKey, Integer> descriptor =
          new MapStateDescriptor<>("keyValueStore", JobKey.class, Integer.class);
      keyValueStore = new FlinkKeyValueStore<>(getRuntimeContext().getMapState(descriptor));
    }

    @Override
    public Integer map(JobKey jobKey) throws Exception {
      if (keyValueStore.get(jobKey) == null) {
        keyValueStore.put(jobKey, 1);
      } else {
        keyValueStore.put(jobKey, keyValueStore.get(jobKey) + 1);
      }
      return keyValueStore.get(jobKey);
    }
  }

}
