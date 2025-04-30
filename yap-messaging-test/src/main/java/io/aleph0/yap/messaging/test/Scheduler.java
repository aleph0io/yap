package io.aleph0.yap.messaging.test;

import java.time.Duration;
import java.util.Random;

/**
 * A source of delays useful for scheduling message delivery in tests.
 */
@FunctionalInterface
public interface Scheduler {
  /**
   * Returns a scheduler that delays messages by a random amount between 80ms and 120ms.
   * 
   * @return the scheduler
   */
  public static Scheduler defaultScheduler() {
    return defaultScheduler(new Random());
  }

  /**
   * Returns a scheduler that delays messages by a random amount between 80ms and 120ms using the
   * given random number generator.
   * 
   * @return the scheduler
   */
  public static Scheduler defaultScheduler(Random rand) {
    return randomScheduler(rand, 80, 40);
  }

  /**
   * Returns a scheduler that delays messages by a random amount between {@code base} and
   * {@code base + jitter} at millisecond precision using the given random number generator.
   * 
   * @return the scheduler
   */
  public static Scheduler randomScheduler(Random rand, long base, long jitter) {
    if (rand == null)
      throw new NullPointerException("rand");
    if (base < 0)
      throw new IllegalArgumentException("base must be non-negative");
    if (jitter < 0)
      throw new IllegalArgumentException("jitter must be non-negative");

    if (jitter == 0)
      return () -> Duration.ofMillis(base);

    return () -> Duration.ofMillis(base + rand.nextLong(jitter));
  }

  /**
   * Returns the delay until the value should be scheduled. Must not be negative.
   * 
   * @return the non-negative delay until the value should be scheduled
   */
  public Duration schedule();
}
