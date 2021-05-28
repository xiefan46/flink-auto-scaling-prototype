package org.apache.flink.asc.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;


/**
 * Encapsulates the jobName and instanceID of a job, used to index all attempts of a job (sorted in time).
 */
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class JobNameInstanceIDPair {

  @Getter
  @NonNull
  private final String jobName;

  @Getter
  @NonNull
  private final String instanceID;

  // Default constructor for serde
  private JobNameInstanceIDPair() {
    this("", "");
  }
}
