package com.linkedin.asc.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@ToString
@EqualsAndHashCode
public class SizingAction {

  public enum Status { ENQUEUED, ISSUED, COMPLETE, DISCARDED }

  // Enum for the type of action -- that is, which job-sizing parameter was changed
  public enum Type {
    JOB_HEAP_MEMORY_SCALEUP,
    JOB_HEAP_MEMORY_SCALEDOWN,
    JOB_TOTAL_MEMORY_SCALEUP,
    JOB_TOTAL_MEMORY_SCALEDOWN,
    JOB_VCORE_SCALEUP,
    JOB_VCORE_SCALEDOWN,
    JOB_THREAD_SCALEUP,
    JOB_THREAD_SCALEDOWN
  }

  @Getter
  @Setter
  public Status status;

  @Getter
  public final JobKey jobKey;

  @Getter
  public final JobSize currentJobSize;

  @Getter
  public final JobSize targetJobSize;

  @Getter
  public final Type type;

  // Default constructor for allowing serde via jackson
  private SizingAction() {
    this(null, null, null, Status.ENQUEUED, null);
  }

  public SizingAction(JobKey job, JobSize currentJobSize, JobSize targetJobSize, Type type) {
    this(job, currentJobSize, targetJobSize, Status.ENQUEUED, type);
  }

  public SizingAction(JobKey job, JobSize currentJobSize, JobSize targetJobSize, Status status, Type type) {
    this.jobKey = job;
    this.currentJobSize = currentJobSize;
    this.targetJobSize = targetJobSize;
    this.status = status;
    this.type = type;
  }
}
