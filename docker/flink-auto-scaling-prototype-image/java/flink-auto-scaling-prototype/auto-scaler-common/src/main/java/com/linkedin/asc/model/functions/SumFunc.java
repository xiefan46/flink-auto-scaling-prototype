package com.linkedin.asc.model.functions;

public class SumFunc implements FoldLeftFunction<Double, Double> {
  public static final FoldLeftFunction<Double, Double> SUM_FUNC = new SumFunc();

  private SumFunc() {
  }

  @Override
  public Double apply(Double value1, Double value2) {
    return value1 + value2;
  }
}
