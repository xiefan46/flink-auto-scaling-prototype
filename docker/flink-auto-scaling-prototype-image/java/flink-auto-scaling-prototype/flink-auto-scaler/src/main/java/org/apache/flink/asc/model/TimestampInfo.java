package org.apache.flink.asc.model;

import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;


public class TimestampInfo {

  /**
   * The value of the event-time value in the first message read from the diagnostic stream header that concerns a given data.
   */
  @Getter
  protected long firstTime;

  /**
   * The value of the event-time value in the last message read from the diagnostic stream header that concerns a given data.
   */
  @Getter
  protected long lastTime;

  public void setLastTime(long timestamp) {
    this.lastTime = timestamp;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    TimestampInfo that = (TimestampInfo) other;
    return firstTime == that.firstTime && lastTime == that.lastTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(firstTime, lastTime);
  }
}
