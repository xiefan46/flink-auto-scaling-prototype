package org.apache.flink.model;


public interface MetricHeader {

  String getJobId();

  String getJobAttemptId();

  String getContainerName();

}
