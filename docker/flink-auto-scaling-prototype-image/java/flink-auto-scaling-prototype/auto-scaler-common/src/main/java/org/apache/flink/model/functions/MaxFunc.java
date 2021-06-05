package org.apache.flink.model.functions;

import org.apache.flink.model.TimeWindow;
import lombok.EqualsAndHashCode;


/**
 * This class is used by {@link TimeWindow} to aggregate metrics
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
