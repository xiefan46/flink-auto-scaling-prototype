package com.linkedin.asc.model;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;



public interface MetricHeader {

  String getJobId();

  String getJobAttemptId();

  String getContainerName();

}
