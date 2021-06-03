package com.linkedin.asc.model;

import com.linkedin.asc.model.functions.FoldLeftFunction;
import com.linkedin.asc.model.functions.MaxFunc;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;


/**
 * Encapsulates logic to maintain a sliding window of metric values within certain time period.
 * For example, to store value from different containers of the same job within 12 hours.
 *
 * The time maxLength of the window is configurable. The granularity with which the timestamp of the incoming values is to
 * be stored is also configurable, e.g., 1 minute.
 * If there are multiple metric values with the timestamps within the same minute,
 * those values will be aggregated based on the aggregate function.
 */
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class TimeWindow {

  @Getter
  // Length in time of the window
  private Duration maxLength;

  // Granularity of the timestamp of the metric values
  private static final TemporalUnit TIMESTAMP_GRANULARITY = ChronoUnit.MINUTES;

  private final TreeMap<Instant, Double> metricWindow = new TreeMap<>();

  // Private default constructor to enable serde
  private TimeWindow() {
    this(Duration.ofSeconds(0));
  }

  public void addMetric(Instant timestamp, Double value, FoldLeftFunction<Double, Double> aggregateFunc) {
    timestamp = timestamp.truncatedTo(TIMESTAMP_GRANULARITY);
    Double existingValue = metricWindow.get(timestamp);
    if (existingValue == null) {
      metricWindow.put(timestamp, value);
    } else {
      metricWindow.put(timestamp, aggregateFunc.apply(value, existingValue));
    }

    while (Duration.between(metricWindow.firstKey(), metricWindow.lastKey()).compareTo(maxLength) > 0) {
      metricWindow.remove(metricWindow.firstKey());
    }
  }

  /**
   * Set the length of the metric window
   * @param metricWindowLength the length in time of this metric window
   */
  public void setLength(Duration metricWindowLength) {
    this.maxLength = metricWindowLength;
  }

  /**
   * Returns a sub-window of the current TimeWindow
   * Notice that the end point of this sub-window is the same as the current TimeWindow.
   * @param windowSize
   * @return
   */
  public TimeWindow getTrailingWindow(Duration windowSize) {
    Instant windowEnd = metricWindow.lastKey();
    Instant windowStart = windowEnd.minus(windowSize.toMinutes(), TIMESTAMP_GRANULARITY);
    TimeWindow subWindow = new TimeWindow(windowSize);
    return getSubWindow(windowStart, windowEnd);
  }

  /**
   * Returns a sub-window of the current TimeWindow given a start and end time of the sub-window
   */
  public TimeWindow getSubWindow(Instant windowStart, Instant windowEnd) {
    TimeWindow
        subWindow = new TimeWindow(Duration.ofMillis(windowEnd.toEpochMilli() - windowStart.toEpochMilli()));
    for (Map.Entry<Instant, Double> value : metricWindow.subMap(windowStart, true, windowEnd, true).entrySet()) {
      subWindow.addMetric(value.getKey(), value.getValue(), MaxFunc.MAX_FUNC);
    }
    return subWindow;
  }

  public List<TimestampedValue<Double>> getMetrics() {
    return metricWindow.entrySet()
        .stream()
        .map(entry -> new TimestampedValue<>(entry.getValue(), entry.getKey().toEpochMilli()))
        .collect(Collectors.toList());
  }

  public Duration getCurrentLength() {
    if (metricWindow.size() <= 0) {
      return Duration.ofSeconds(0);
    }
    return Duration.between(metricWindow.firstKey(), metricWindow.lastKey());
  }

  /**
   * @return Maximum value across all values in the metric-window
   */
  public double getMax() {
    if (this.getMetrics().size() > 0) {
      return Collections.max(this.getMetrics().stream().map(TimestampedValue::getValue).collect(Collectors.toList()));
    }
    return 0;
  }

  /**
   * @return Maximum value in a per second granularity.
   */
  public double getMaxPerSecondRate() {
    return this.getMax() / TIMESTAMP_GRANULARITY.getDuration().getSeconds();
  }

  /**
   * @return Average value across all values in the metric-window
   */
  public double getAverage() {
    if (this.getMetrics().size() > 0) {
      return this.getMetrics().stream().mapToDouble(TimestampedValue::getValue).summaryStatistics().getAverage();
    }
    return 0;
  }
}
