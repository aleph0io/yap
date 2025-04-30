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
package io.aleph0.yap.messaging.core;

import static java.util.Objects.requireNonNull;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.core.worker.MeasuredConsumerWorker;
import io.aleph0.yap.messaging.core.Acknowledgeable.AcknowledgementListener;

/**
 * A {@link ProcessorWorker} that receives inputs from upstream, acknowledges the input, and then
 * discards the input.
 *
 * <p>
 * On source failure, the worker will fail the task.
 * 
 * <p>
 * On acknowledgement failure, the worker may fail the task, at the discretion of its
 * {@link AcknowledgementFailureHandler failure handler}.
 * 
 * <p>
 * On interrupt, the worker will stop receiving messages immediately.
 * 
 * <p>
 * In all cases, the worker will wait for all outstanding acks to finish before stopping or failing
 * the task.
 *
 * @param <T> the type of input messages
 */
public class AcknowledgerConsumerWorker<T extends Acknowledgeable>
    implements MeasuredConsumerWorker<T, AcknowledgerMetrics> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AcknowledgerConsumerWorker.class);

  /**
   * Allows the user to customize the behavior of the worker when an acknowledgement fails.
   */
  @FunctionalInterface
  public static interface AcknowledgementFailureHandler {
    /**
     * Called when an {@link Acknowledgeable#ack(AcknowledgementListener) ack} fails. If the user
     * wants the task to continue, then they should simply return. If the user wants the task to
     * fail, then they should throw a {@link Throwable}, either the given one or a new one.
     * 
     * <p>
     * The underlying implementation handles {@link InterruptedException} and {@link Error}
     * instances for correctness, so those causes cannot be intercepted. Also, as a rule, users
     * should not throw these types from this method.
     * 
     * @param t the failure cause
     * @throws Throwable the failure cause, if the user wants the task to fail
     */
    void onAckFailure(Throwable t) throws Throwable;
  }

  /**
   * Returns the default {@link AcknowledgementFailureHandler}, which simply re-throws the given
   * failure cause.
   * 
   * @return the default {@link AcknowledgementFailureHandler}
   */
  public static AcknowledgementFailureHandler defaultAcknowledgementFailureHandler() {
    return t -> {
      throw t;
    };
  }

  private final AtomicLong acknowledgedMetric = new AtomicLong(0);
  private final AtomicLong retiredSuccessMetric = new AtomicLong(0);
  private final AtomicLong retiredFailureMetric = new AtomicLong(0);
  private final AtomicLong awaitingMetric = new AtomicLong(0);
  private final Phaser phaser = new Phaser(1);

  private final AcknowledgementFailureHandler acknowledgementFailureHandler;

  public AcknowledgerConsumerWorker() {
    this(defaultAcknowledgementFailureHandler());
  }

  public AcknowledgerConsumerWorker(AcknowledgementFailureHandler acknowledgementFailureHandler) {
    this.acknowledgementFailureHandler =
        requireNonNull(acknowledgementFailureHandler, "acknowledgementFailureHandler");
  }

  @Override
  public void consume(Source<T> source) throws IOException, InterruptedException {
    try {
      try {
        final AtomicReference<Throwable> failureCause = new AtomicReference<>(null);

        for (T input = source.take(); input != null; input = source.take()) {
          // Check if we have a failure
          throwIfPresent(failureCause);

          // Set up acknowledgement
          acknowledgedMetric.incrementAndGet();
          awaitingMetric.incrementAndGet();
          phaser.register();

          // Do the acknowledgement
          input.ack(new Acknowledgeable.AcknowledgementListener() {
            @Override
            public void onSuccess() {
              retiredSuccessMetric.incrementAndGet();
              awaitingMetric.decrementAndGet();
              phaser.arriveAndDeregister();
            }

            @Override
            public void onFailure(Throwable originalCause) {
              retiredFailureMetric.incrementAndGet();
              awaitingMetric.decrementAndGet();
              phaser.arriveAndDeregister();
              if (originalCause instanceof Error) {
                LOGGER.atError().setCause(originalCause)
                    .log("Error while trying to acknowledge message. Failing task...");
                failureCause.compareAndSet(null, originalCause);
              } else if (originalCause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                LOGGER.atWarn().setCause(originalCause)
                    .log("Interrupted while trying to acknowledge message. Stopping...");
                failureCause.compareAndSet(null, new InterruptedException());
              } else {
                try {
                  acknowledgementFailureHandler.onAckFailure(originalCause);
                  LOGGER.atWarn().setCause(originalCause)
                      .log("Failed to acknowledge message, but user ignored. Continuing...");
                } catch (Throwable t) {
                  LOGGER.atError().setCause(t)
                      .log("Failed to acknowledge message and user propagated. Failing task...");
                  failureCause.compareAndSet(null, t);
                }
              }
            }
          });
        }

        throwIfPresent(failureCause);
      } finally {
        phaser.arriveAndAwaitAdvance();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.atWarn().setCause(e)
          .log("Interrupted while waiting for acknowledgements. Propagating...");
      throw e;
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      LOGGER.atError().setCause(cause).log("Pubsub firehose failed. Failing task...");
      if (cause instanceof Error x)
        throw x;
      if (cause instanceof IOException x)
        throw x;
      if (cause instanceof RuntimeException x)
        throw x;
      if (cause instanceof Exception x)
        throw new IOException("Pubsub firehose failed", x);
      throw new AssertionError("Unexpected error", e);
    }
  }

  private void throwIfPresent(AtomicReference<Throwable> failureCause)
      throws InterruptedException, ExecutionException {
    Throwable fc = failureCause.get();
    if (fc != null) {
      if (fc instanceof Error x)
        throw x;
      if (fc instanceof InterruptedException)
        throw new InterruptedException();
      if (fc instanceof Exception e)
        throw new ExecutionException(e);
      throw new AssertionError("Unexpected error", fc);
    }
  }

  @Override
  public AcknowledgerMetrics checkMetrics() {
    final long acknowledged = acknowledgedMetric.get();
    final long retiredSuccess = retiredSuccessMetric.get();
    final long retiredFailure = retiredFailureMetric.get();
    final long retired = retiredSuccess + retiredFailure;
    final long awaiting = awaitingMetric.get();
    return new AcknowledgerMetrics(acknowledged, retired, retiredSuccess, retiredFailure, awaiting);
  }

  @Override
  public AcknowledgerMetrics flushMetrics() {
    final AcknowledgerMetrics metrics = checkMetrics();
    acknowledgedMetric.set(0);
    retiredSuccessMetric.set(0);
    retiredFailureMetric.set(0);
    return metrics;
  }
}
