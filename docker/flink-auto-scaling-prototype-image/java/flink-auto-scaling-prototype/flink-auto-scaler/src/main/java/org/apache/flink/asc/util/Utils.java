package org.apache.flink.asc.util;

import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.asc.model.JobKey;
import org.apache.flink.asc.model.JobState;
import org.apache.flink.asc.model.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Utils {

  private final static Logger LOG = LoggerFactory.getLogger(Utils.class.getName());
  public final static int KB = 1024;
  public final static int MB = KB * KB;
  public final static int GB = KB * MB;
  public static final int CONTAINER_MEMORY_UNIT_MB = 1024;

  // Private default constructor for util
  private Utils() {
  }




  // Helper method to read everything into
  public static String readAll(Reader rd) throws IOException {
    StringBuilder sb = new StringBuilder();
    int cp;
    while ((cp = rd.read()) != -1) {
      sb.append((char) cp);
    }
    return sb.toString();
  }

  /**
   * Shutdown an executor with a given shutdown-wait timeout
   * @param executorService the executor service to shutdown
   * @param shutdownWaitTimeout the time to wait after calling shutdown and before calling shutdownNow()
   */
  public static void shutdownExecutor(ExecutorService executorService, Duration shutdownWaitTimeout) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(shutdownWaitTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        List<Runnable> runnables = executorService.shutdownNow();
        LOG.info("List of actions terminated: {}", runnables);
      }
    } catch (InterruptedException e) {
      LOG.error("Exception during shutdown: {}", e);
    }
  }

  /**
   * Pick a random element from the given list of items
   * @param items item list to pick a random element from
   * @param <E> type of the elements
   * @return the randomly picked element
   */
  public static <E> E pickRandom(List<E> items) {
    if (items == null || items.size() == 0) {
      return null;
    }
    return items.get(new Random().nextInt(items.size()));
  }

  /**
   * Check if the job has been running for more than the given period.
   * @return
   */
  public static boolean initialPeriodExpired(JobKey jobKey, JobState jobState, Duration period) {
    boolean retVal = jobState.getLastTime() - jobState.getFirstTime() > period.toMillis();
    LOG.info("Checking if initial period {} expired on jobState: {}, returning: {}, job: {}", period, jobState, retVal,
        jobKey);
    return retVal;
  }

  /**
   * Check if the job's timeWindow has values spanning its configured length, and the window's start is not within
   * initializationPeriod from the start time of the job.
   */
  public static boolean isValidWindow(JobKey jobKey, JobState jobState, TimeWindow timeWindow,
      Duration initializationPeriod) {
    if (timeWindow == null || timeWindow.getCurrentLength().isZero()) {
      LOG.info("Null or empty timeWindow for job: {}", jobKey);
      return false;
    } else if (timeWindow.getCurrentLength().compareTo(timeWindow.getMaxLength()) < 0) {
      LOG.info("TimeWindow for job: {} is only partially full {}", jobKey, timeWindow);
      return false;
    } else if (timeWindow.getMetrics().get(0).getTimestamp()
        < jobState.getFirstTime() + initializationPeriod.toMillis()) {
      LOG.info("TimeWindow for job: {} spans initialization delay, {} is less than {}", jobKey,
          timeWindow.getMetrics().get(0).getTimestamp(), jobState.getFirstTime() + initializationPeriod.toMillis());
      return false;
    }
    return true;
  }

  /**
   * Helper function to determine if two values are within a limit of each other.
   * @param smallerValue the smaller value.
   * @param largerValue the larger value.
   * @param fractionDifferenceLimit the max allowable fractional difference between the two values.
   * @param differenceLimit the max allowable difference between the two values.
   * @return True if either the fractional-difference or the actual difference is lower than the respective limits,
   * False otherwise.
   */
  public static boolean isWithinLimit(double smallerValue, double largerValue, double fractionDifferenceLimit,
      int differenceLimit) {
    double difference = largerValue - smallerValue;
    double differenceFraction = difference / largerValue;
    return (differenceFraction <= fractionDifferenceLimit) || difference <= differenceLimit;
  }


  /**
   * Helper method to truncated given timestamp to the minute.
   */
  public static Instant roundOffToMinute(Instant timestamp) {
    return timestamp.truncatedTo(ChronoUnit.MINUTES);
  }

}
