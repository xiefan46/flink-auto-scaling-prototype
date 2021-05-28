package org.apache.flink.asc.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class JobKey {
  @Getter
  private final String jobName;

  @Getter
  private final String jobId;

  //TODO
  public String getInstanceID() {
    return null;

  }
}
