package org.apache.flink.asc.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;



/**
 * Encapsulates properties concerning the runtime state of a job.
 */
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class JobState extends TimestampInfo {

  // Sizing-related parameters of the job
  @Getter
  private final JobSize jobSize;

  // Number of stores the job uses that are configured with a changelog
  @Getter
  private final int numPersistentStores;

  // Number of task instances of this job
  @Getter
  private final int numTasks;




}
