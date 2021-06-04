package com.linkedin.asc.model;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.asc.util.Utils.*;


/**
 * Encapsulate all configurable, sizing-related parameters of a job.
 */
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class JobSize {
  private static final Logger LOG = LoggerFactory.getLogger(JobSize.class);

  // Default constructor for jackson serde
  private JobSize() {
    this(0, 0, 0);
  }

  @Getter
  private final int containerMb;

  @Getter
  private final int containerNumCores;

  @Getter
  private final int containerCount;

  @JsonIgnore
  public int getTotalMemoryMb() {
    return this.containerMb * this.containerCount;
  }


  @JsonIgnore
  /**
   * Get the total number of cores allocated to this job.
   */ public int getTotalNumCores() {
    return this.containerNumCores * this.containerCount;
  }
}
