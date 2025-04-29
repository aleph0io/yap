/*-
 * =================================LICENSE_START==================================
 * yap-messaging-gcp
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
package io.aleph0.yap.messaging.gcp;

import static java.util.Objects.requireNonNull;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.core.task.TaskController;
import io.aleph0.yap.messaging.core.RelayMetrics;
import io.aleph0.yap.messaging.core.RelayProcessorWorker;

/**
 * A {@link RelayProcessorWorker relay} that receives inputs from upstream, extracts zero or more
 * messages from each input, publishes the extracted message(s) to Google Pubsub if any, and then
 * publishes the original input to downstream.
 * 
 * <p>
 * To ensure message delivery, this worker fails immediately if the publisher fails to publish a
 * message. If message delivery failure is tolerable, then they can use a custom
 * {@link TaskController} for this task to allow for worker retry.
 */
public class PubsubRelayProcessorWorker<ValueT> implements RelayProcessorWorker<ValueT> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubsubRelayProcessorWorker.class);

  /**
   * A factory for creating a {@link Publisher} that will publish messages to PubSub. Each worker
   * will call this factory to create a new publisher exactly once. The factory should return the
   * publisher the worker should use to publish messages. Because publishers have a lifecycle that
   * this worker manages to ensure message delivery, the factory should return a new publisher each
   * time.
   */
  @FunctionalInterface
  public static interface PublisherFactory {

    /**
     * Creates a new {@link Publisher} that will publish messages to PubSub. This method should
     * return a new publisher each time it is called.
     * 
     * @return the new publisher
     */
    public Publisher newPublisher();
  }

  @FunctionalInterface
  public static interface MessageExtractor<T> {
    public List<PubsubMessage> extractMessages(T value);
  }

  private final AtomicLong submittedMetric = new AtomicLong(0);
  private final AtomicLong acknowledgedMetric = new AtomicLong(0);

  /**
   * We start at 1 so we can await in the process method after the last message is processed
   */
  private final Phaser phaser = new Phaser(1);

  /**
   * The PubSub client to which we publish messages
   */
  private final PublisherFactory publisherFactory;

  private final MessageExtractor<ValueT> messageExtractor;

  public PubsubRelayProcessorWorker(PublisherFactory publisherFactory,
      MessageExtractor<ValueT> messageExtractor) {
    this.publisherFactory = requireNonNull(publisherFactory, "publisherFactory");
    this.messageExtractor = requireNonNull(messageExtractor, "messageExtractor");
  }

  @Override
  public void process(Source<ValueT> source, Sink<ValueT> sink)
      throws IOException, InterruptedException {
    try {
      final Publisher publisher = publisherFactory.newPublisher();
      try {
        final AtomicReference<Throwable> failureCause = new AtomicReference<>(null);
        for (ValueT input = source.take(); input != null; input = source.take()) {
          final List<PubsubMessage> messages = messageExtractor.extractMessages(input);
          for (PubsubMessage message : messages) {
            // Throw the failure cause if we have one
            throwIfPresent(failureCause);

            // Publish the message to the PubSub
            ApiFuture<String> future = publisher.publish(message);

            submittedMetric.incrementAndGet();

            phaser.register();

            final ValueT theinput = input;
            ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
              @Override
              public void onSuccess(String result) {
                try {
                  acknowledgedMetric.incrementAndGet();

                  phaser.arriveAndDeregister();

                  sink.put(theinput);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  LOGGER.atWarn().addKeyValue("message", e.getMessage()).setCause(e)
                      .log("Interrupted while trying to put published message. Failing task...");
                  failureCause.compareAndSet(null, e);
                } catch (Throwable e) {
                  LOGGER.atError().addKeyValue("message", e.getMessage()).setCause(e)
                      .log("Failed to put published message. Failing task...");
                  failureCause.compareAndSet(null, e);
                }
              }

              @Override
              public void onFailure(Throwable t) {
                LOGGER.atError().addKeyValue("message", t.getMessage()).setCause(t)
                    .log("Message failed to publish. Failing task...");

                failureCause.compareAndSet(null, t);

                acknowledgedMetric.incrementAndGet();

                phaser.arriveAndDeregister();
              }
            }, MoreExecutors.directExecutor());
          }
        }

        throwIfPresent(failureCause);

        phaser.arriveAndAwaitAdvance();
      } finally {
        publisher.shutdown();
      }
    } catch (InterruptedException e) {
      // Propagate the interruption
      Thread.currentThread().interrupt();
      LOGGER.atWarn().setCause(e).log("Interrupted. Failing task...");
      throw e;
    } catch (RuntimeException e) {
      LOGGER.atError().setCause(e).log("Pubsub firehose failed. Failing task...");
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
      if (fc instanceof Error e)
        throw e;
      if (fc instanceof InterruptedException e)
        throw e;
      if (fc instanceof Exception e)
        throw new ExecutionException(e);
      throw new AssertionError("Unexpected error", fc);
    }
  }

  public RelayMetrics checkMetrics() {
    final long submitted = submittedMetric.getAndSet(0L);
    final long acknowledged = acknowledgedMetric.getAndSet(0L);
    final long awaiting = phaser.getUnarrivedParties() - 1;
    return new RelayMetrics(submitted, acknowledged, awaiting);
  }

  public RelayMetrics flushMetrics() {
    RelayMetrics metrics = checkMetrics();
    submittedMetric.set(0);
    acknowledgedMetric.set(0);
    return metrics;
  }
}
