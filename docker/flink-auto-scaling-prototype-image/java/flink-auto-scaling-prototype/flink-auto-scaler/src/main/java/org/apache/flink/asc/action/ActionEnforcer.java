package org.apache.flink.asc.action;

import org.apache.flink.asc.model.SizingAction;


/**
 * Receives {@link SizingAction}s and applies them to the underlying job.
 */
public interface ActionEnforcer {

  /**
   * Synchronously applies the action, depending on the underlying implementation.
   * @param sizingAction
   */
  void applyAction(SizingAction sizingAction) throws Exception;

  /**
   * Applies the action and disable auto-sizing at the same time
   * @param sizingAction
   */
  void applyActionAndDisableAutoSizing(SizingAction sizingAction) throws Exception;
}
