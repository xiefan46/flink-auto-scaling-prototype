package org.apache.flink.asc.store;

import com.linkedin.asc.model.JobKey;
import java.util.LinkedList;
import lombok.Getter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestFlinkKeyValueStore {
  private KeyedOneInputStreamOperatorTestHarness<Integer, JobKey, Integer> testHarness;
  private StatefulMapFunction statefulMapFunction;

  @Before
  public void setupTestHarness() throws Exception {

    //instantiate user-defined function
    statefulMapFunction = new StatefulMapFunction();

    // wrap user defined function into a the corresponding operator
    OneInputStreamOperator<JobKey, Integer> operator = new StreamMap(statefulMapFunction);
    testHarness = new KeyedOneInputStreamOperatorTestHarness<Integer, JobKey, Integer>(operator, new KeySelector<JobKey, Integer>() {
      @Override
      public Integer getKey(JobKey jobKey) throws Exception {
        return 0;
      }
    }, TypeInformation.of(new TypeHint<Integer>() {
    }));

    // optionally configured the execution environment
    testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

    // open the test harness (will also call open() on RichFunctions)
    testHarness.open();
  }

  @Test
  public void testingFlinkKeyValueState() throws Exception {

    JobKey jobKey = new JobKey("test-job", "attempt-01");
    JobKey jobKey2 = new JobKey("test-job-2", "attempt-01");
    testHarness.processElement(jobKey, 100L);
    testHarness.processElement(jobKey, 101L);
    testHarness.processElement(jobKey2, 101L);
    testHarness.processElement(jobKey, 102L);
    Assert.assertEquals(new Integer(3), statefulMapFunction.getKeyValueStore().get(jobKey));
    Assert.assertEquals(new Integer(1), statefulMapFunction.getKeyValueStore().get(jobKey2));
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
