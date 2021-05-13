package com.linkedin.asc.model.functions;


@FunctionalInterface
public interface FoldLeftFunction<M, WV>{

  /**
   * Incrementally updates the aggregated value as messages are added.
   *
   * @param message the message being added to the aggregated value
   * @param oldValue the previous value
   * @return the new value
   */
  WV apply(M message, WV oldValue);
}
