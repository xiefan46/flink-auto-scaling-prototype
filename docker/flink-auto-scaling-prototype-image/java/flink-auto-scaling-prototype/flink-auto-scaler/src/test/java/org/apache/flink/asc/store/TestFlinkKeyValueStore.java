package org.apache.flink.asc.store;

import com.linkedin.asc.model.JobKey;
import lombok.Getter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;


public class TestFlinkKeyValueStore {
  private KeyedOneInputStreamOperatorTestHarness<String, JobKey, String> testHarness;
  private StatefulMapFunction statefulMapFunction;

  @Before
  public void setupTestHarness() throws Exception {

    //instantiate user-defined function
    statefulMapFunction = new StatefulMapFunction();

    // wrap user defined function into a the corresponding operator
    testHarness = new KeyedOneInputStreamOperatorTestHarness<>(

    );

    // optionally configured the execution environment
    testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

    // open the test harness (will also call open() on RichFunctions)
    testHarness.open();
  }

  @Test
  public void testingStatefulFlatMapFunction() throws Exception {

    //push (timestamped) elements into the operator (and hence user defined function)
    String key = "test-job";
    testHarness.processElement(key, 100L);
    testHarness.processElement(key, 100L);
    Assert.assertEquals(statefulMapFunction.getKeyValueStore().get(key), new Integer(2));

    //retrieve list of records emitted to a specific side output for assertions (ProcessFunction only)
    //assertThat(testHarness.getSideOutput(new OutputTag<>("invalidRecords")), hasSize(0))
  }

  class StatefulMapFunction extends RichMapFunction<String, String> {

    @Getter
    FlinkKeyValueStore<String, Integer> keyValueStore;

    @Override
    public void open(Configuration config) {
      MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<String, Integer>("keyValueStore",
          String.class, Integer.class);
      keyValueStore = new FlinkKeyValueStore<>(getRuntimeContext().getMapState(descriptor));
    }

    @Override
    public String map(String s) throws Exception {
      if(keyValueStore.get(s) == null) {
        keyValueStore.put(s, 1);
      }else{
        keyValueStore.put(s, keyValueStore.get(s) + 1);
      }
      return s;
    }
  }
}
