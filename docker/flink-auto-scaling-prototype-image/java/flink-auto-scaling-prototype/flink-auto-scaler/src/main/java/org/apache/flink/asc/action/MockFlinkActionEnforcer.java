package org.apache.flink.asc.action;

import com.linkedin.asc.action.ActionEnforcer;
import com.linkedin.asc.model.SizingAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Prints out the sizing actions instead of executing those actions in real
 */
public class MockFlinkActionEnforcer implements ActionEnforcer {

  private static final Logger LOG = LoggerFactory.getLogger(MockFlinkActionEnforcer.class);

  @Override
  public void applyAction(SizingAction sizingAction) throws Exception {
    LOG.info("Apply sizing action. SizingAction: {}", sizingAction);
  }

  @Override
  public void applyActionAndDisableAutoScaling(SizingAction sizingAction) throws Exception {
    LOG.info("Apply sizing action and disable auto-sizing. SizingAction: {}", sizingAction);
  }
}
