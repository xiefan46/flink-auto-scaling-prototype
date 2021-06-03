package com.linkedin.asc.action;

import com.linkedin.asc.model.JobKey;
import com.linkedin.asc.model.SizingAction;


/**
 * ActionRegistry is the interface to interact with ActionEnforcers.
 * Registering a desired actions with the ActionRegistry will propagate them to ActionEnforce(s).
 * Actionregistr can also be queries to check if there is a pending-action for a given jobKey.
 */
public interface ActionRegistry {

  /**
   * Register the given sizing action for application.
   * @param action
   */
  void registerAction(SizingAction action);

  /**
   * Check if an action has already been registered for a given jobname-instanceID-attemptID.
   * @param jobKey
   * @return true, if there is any action for this job, false, otherwise
   */
  boolean hasAction(JobKey jobKey);

  /**
   * Shutdown this action registry.
   */
  void shutdown();
}
