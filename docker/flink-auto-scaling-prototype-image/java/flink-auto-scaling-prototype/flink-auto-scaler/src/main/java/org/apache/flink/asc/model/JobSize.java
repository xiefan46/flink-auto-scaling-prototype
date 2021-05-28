package org.apache.flink.asc.model;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.asc.util.Utils.*;

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
    this(0, 0, 0, 0, 0);
  }

  // The factor to be multiplied by the desired heap size to get the Xmx setting. This accounts for the space reserved for GC
  // by the JVM
  // See https://iwww.corp.linkedin.com/wiki/cf/pages/viewpage.action?pageId=298880836 for details
  private static final float GC_SPACE_FACTOR_FOR_JVM_HEAP = 1.126f;

  @Getter
  private final int containerMb;

  @Getter
  private final int containerNumCores;

  @Getter
  private final int containerCount;

  @Getter
  private long containerMaxHeapSizeBytes;

  @Getter
  private final int containerThreadPoolSize;

  @JsonIgnore
  public int getTotalMemoryMb() {
    return this.containerMb * this.containerCount;
  }

  @JsonIgnore
  public int getTotalHeapMemoryMb() {
    return ((int) (this.containerMaxHeapSizeBytes / MB)) * this.containerCount;
  }

  @JsonIgnore
  public long getTotalHeapMemoryBytes() {
    return this.containerMaxHeapSizeBytes * this.containerCount;
  }

  @JsonIgnore
  public int getTotalThreadPoolSize() {
    return this.containerThreadPoolSize * this.containerCount;
  }

  @JsonIgnore
  public void updateContainerMaxHeapBytes(long containerMaxHeapSizeBytes) {
    this.containerMaxHeapSizeBytes = containerMaxHeapSizeBytes;
  }

  @JsonIgnore
  public int getContainerMaxHeapSizeMb() {
    return (int) (this.containerMaxHeapSizeBytes / MB);
  }

  /**
   * Get the likely Xmx setting for the given containerMaxHeapSizeBytes value.
   * A given containerMaxHeapSizeBytes value does not translate directly to the Xmx setting.
   * This is because a JVM reserves about 12% of heap for its use.
   * See https://stackoverflow.com/questions/23701207/why-do-xmx-and-runtime-maxmemory-not-agree
   * for details. So we account for this addition when applying the Xmx setting for the desired max-heap-size.
   *
   * @return the Xmx setting setting in MB to get the current containerMaxHeapSizeBytes
   */
  @VisibleForTesting
  @JsonIgnore
  public int getContainerXmxMb() {
    int targetHeapSizeMb = Math.round(GC_SPACE_FACTOR_FOR_JVM_HEAP * containerMaxHeapSizeBytes / MB);
    LOG.info("Xmx value: {} for target containerMaxHeapSizeBytes: {}", targetHeapSizeMb, containerMaxHeapSizeBytes);
    return targetHeapSizeMb;
  }

  /**
   * Helper function to get the max heap size associated with a given Xmx value.
   * @param xmxValue given jvm Xmx value.
   * @return
   */
  public static int getMaxHeapSizeMbForXmx(int xmxValue) {
    int maxHeapSizeMb = Math.round(xmxValue / GC_SPACE_FACTOR_FOR_JVM_HEAP);
    LOG.info("maxHeapSizeMb: {} for Xmx value: {}", maxHeapSizeMb, xmxValue);
    return maxHeapSizeMb;
  }

  @JsonIgnore
  /**
   * Get the total number of cores allocated to this job.
   */ public int getTotalNumCores() {
    return this.containerNumCores * this.containerCount;
  }
}
