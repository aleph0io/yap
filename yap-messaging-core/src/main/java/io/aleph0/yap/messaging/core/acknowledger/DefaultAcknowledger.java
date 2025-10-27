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
package io.aleph0.yap.messaging.core.acknowledger;

import static java.util.Objects.requireNonNull;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.messaging.core.Acknowledgeable;
import io.aleph0.yap.messaging.core.AcknowledgementFailureHandler;
import io.aleph0.yap.messaging.core.Acknowledger;
import io.aleph0.yap.messaging.core.AcknowledgerMetrics;

/**
 * Default implementation of the {@link Acknowledger} interface. This class handles the mechanics of
 * acknowledging messages, including tracking metrics and handling failures. The class is designed
 * to be thread-safe and can be used in concurrent environments. When message acknowledgement is
 * desired, this should generally be the default implementation used.
 * 
 * @param <T> the type of {@link Acknowledgeable} that this acknowledger will handle
 */
public class DefaultAcknowledger<T extends Acknowledgeable> implements Acknowledger<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAcknowledger.class);

  private final AtomicLong acknowledgedMetric = new AtomicLong(0);
  private final AtomicLong retiredSuccessMetric = new AtomicLong(0);
  private final AtomicLong retiredFailureMetric = new AtomicLong(0);
  private final AtomicLong awaitingMetric = new AtomicLong(0);
  private final Phaser phaser = new Phaser(1);
  private final AtomicReference<Throwable> failureCause = new AtomicReference<>(null);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final AcknowledgementFailureHandler acknowledgementFailureHandler;

  public DefaultAcknowledger() {
    this(AcknowledgementFailureHandler.defaultAcknowledgementFailureHandler());
  }

  public DefaultAcknowledger(AcknowledgementFailureHandler acknowledgementFailureHandler) {
    this.acknowledgementFailureHandler =
        requireNonNull(acknowledgementFailureHandler, "acknowledgementFailureHandler");
  }

  @Override
  public void acknowledge(Acknowledgeable acknowledgeable) {
    if (closed.get())
      throw new IllegalStateException("closed");

    // If no messages were emitted downstream, we should acknowledge immediately.
    acknowledgedMetric.incrementAndGet();
    awaitingMetric.incrementAndGet();
    phaser.register();

    // Do the acknowledgement
    acknowledgeable.ack(new Acknowledgeable.AcknowledgementListener() {
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

  /**
   * Closes the acknowledger, preventing any new acknowledgements from being initiated. This method
   * will wait for all outstanding acknowledgements to complete before returning. If any
   * acknowledgements fail during this time, the method will throw an exception. If the acknowledger
   * is already closed, this method will do nothing.
   * 
   * @throws InterruptedException if the thread was interrupted while waiting for acknowledgements
   *         to complete.
   * @throws ExecutionException if an exception occurred during acknowledgement that was not handled
   *         by the {@link AcknowledgementFailureHandler}. The cause of the exception will be the
   *         original exception that occurred during acknowledgement.
   * 
   * @see #throwIfPresent() for more details on how exceptions are handled during acknowledgement.
   */
  @Override
  public void close() throws InterruptedException, ExecutionException {
    if (closed.compareAndSet(false, true) == true) {
      // Wait for all acknowledgements to complete
      phaser.arriveAndAwaitAdvance();
      throwIfPresent();
    }
  }

  /**
   * Checks if there is a failure cause present, and if so, throws it. Because acknowledgement is
   * asynchronous, this method should be called periodically during processing to check for
   * failures. Note that any exceptions suppressed by the {@link AcknowledgementFailureHandler} will
   * not be thrown by this method, as they are considered to be handled by the user. Only unhandled
   * exceptions will be thrown.
   * 
   * @throws Error if an error occurred during acknowledgement.
   * @throws InterruptedException if the thread was interrupted while waiting for acknowledgements
   *         to complete.
   * @throws ExecutionException if an exception occurred during acknowledgement that was not handled
   *         by the {@link AcknowledgementFailureHandler}. The cause of the exception will be the
   *         original exception that occurred during acknowledgement.
   */
  @Override
  public void throwIfPresent() throws InterruptedException, ExecutionException {
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
