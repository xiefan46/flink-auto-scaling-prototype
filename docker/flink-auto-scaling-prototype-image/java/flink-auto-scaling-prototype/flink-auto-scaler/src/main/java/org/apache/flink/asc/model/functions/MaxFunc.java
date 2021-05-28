package org.apache.flink.asc.model.functions;

import lombok.EqualsAndHashCode;


/**
 * This class is used by {@link org.apache.flink.asc.model.TimeWindow} to aggregate metrics
 * from different containers into a single value in a specific timestamp.
 */
@EqualsAndHashCode
public class MaxFunc implements FoldLeftFunction<Double, Double> {

  public static final FoldLeftFunction<Double, Double> MAX_FUNC = new MaxFunc();

  private MaxFunc() {
  }

  @Override
  public Double apply(Double message, Double oldValue) {
    return Math.max(message, oldValue);
  }
}
