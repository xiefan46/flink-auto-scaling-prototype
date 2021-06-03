package com.linkedin.asc.model.functions;

import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * Incrementally updates the aggregated value as messages are added. Main usage is in {@link org.apache.samza.operators.windows.Window} operator.
 */
@InterfaceStability.Unstable
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
