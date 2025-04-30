/*-
 * =================================LICENSE_START==================================
 * yap-messaging-core
 * ====================================SECTION=====================================
 * Copyright (C) 2025 aleph0
 * ====================================SECTION=====================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ==================================LICENSE_END===================================
 */
package io.aleph0.yap.messaging.test;

import static java.util.Objects.requireNonNull;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.messaging.core.RelayMetrics;
import io.aleph0.yap.messaging.core.RelayProcessorWorker;

/**
 * A {@link RelayProcessorWorker relay} processor that simulates a "real" relay processor by
 * delaying the processing of messages. This is useful for testing and debugging purposes, as it
 * allows you to simulate the behavior of a relay processor without actually sending the messages
 * anywhere.
 * 
 * @param <ValueT> the type of the value to process
 */
public class TestRelayProcessorWorker<ValueT> implements RelayProcessorWorker<ValueT> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestRelayProcessorWorker.class);


  private final AtomicLong submittedMetrics = new AtomicLong(0);
  private final AtomicLong acknowledgedMetrics = new AtomicLong(0);
  private final AtomicLong awaitingMetrics = new AtomicLong(0);
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  private final Scheduler scheduler;

  /**
   * Creates a new {@link TestRelayProcessorWorker} with the default scheduler.
   * 
   * @see #defaultScheduler()
   */
  public TestRelayProcessorWorker() {
    this(Scheduler.defaultScheduler());
  }

  /**
   * Creates a new {@link TestRelayProcessorWorker} with the given scheduler.
   * 
   * @param scheduler the scheduler to use
   */
  public TestRelayProcessorWorker(Scheduler scheduler) {
    this.scheduler = requireNonNull(scheduler, "scheduler");
  }

  @Override
  public void process(Source<ValueT> source, Sink<ValueT> sink)
      throws IOException, InterruptedException {
    try {
      try {
        final AtomicReference<Throwable> failureCause = new AtomicReference<>(null);
        for (ValueT value = source.take(); value != null; value = source.take()) {
          throwIfPresent(failureCause);

          final Duration delay = scheduler.schedule();
          if (delay.isNegative())
            throw new IllegalArgumentException("scheduler returned negative delay");

          final ValueT thevalue = value;
          executor.schedule(() -> {
            try {
              sink.put(thevalue);
              acknowledgedMetrics.incrementAndGet();
              awaitingMetrics.decrementAndGet();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              LOGGER.atError().setCause(e)
                  .log("Interrupted while trying to put delayed message. Failing task...");
              failureCause.compareAndSet(null, e);
            } catch (Throwable e) {
              LOGGER.atError().setCause(e).log("Failed to put delayed message. Failing task...");
              failureCause.compareAndSet(null, e);
            }
          }, delay.toNanos(), TimeUnit.NANOSECONDS);

          submittedMetrics.incrementAndGet();
          awaitingMetrics.incrementAndGet();
        }

        throwIfPresent(failureCause);
      } finally {
        executor.shutdownNow();
        // I'd rather wait forever, but this is only for testing, so that's fine.
        executor.awaitTermination(30, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.atError().setCause(e).log("Simulated relay interrupted. Failing task...");
      throw e;
    } catch (RuntimeException e) {
      LOGGER.atError().setCause(e).log("Simulated relay failed. Failing task...");
      throw e;
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      LOGGER.atError().setCause(cause).log("Simulated relay failed. Failing task...");
      if (cause instanceof Error x)
        throw x;
      if (cause instanceof IOException x)
        throw x;
      if (cause instanceof RuntimeException x)
        throw x;
      if (cause instanceof Exception x)
        throw new IOException("Simulated relay failed", x);
      throw new AssertionError("Unexpected error", e);
    }
  }

  private void throwIfPresent(AtomicReference<Throwable> failureCause)
      throws InterruptedException, ExecutionException {
    Throwable fc = failureCause.get();
    if (fc != null) {
      if (fc instanceof Error e)
        throw e;
      if (fc instanceof InterruptedException e)
        throw e;
      if (fc instanceof Exception e)
        throw new ExecutionException(e);
      throw new AssertionError("Unexpected error", fc);
    }
  }

  @Override
  public RelayMetrics checkMetrics() {
    final long submitted = submittedMetrics.get();
    final long acknowledged = acknowledgedMetrics.get();
    final long awaiting = awaitingMetrics.get();
    return new RelayMetrics(submitted, acknowledged, awaiting);
  }

  @Override
  public RelayMetrics flushMetrics() {
    final RelayMetrics result = checkMetrics();
    submittedMetrics.set(0);
    acknowledgedMetrics.set(0);
    return result;
  }
}
