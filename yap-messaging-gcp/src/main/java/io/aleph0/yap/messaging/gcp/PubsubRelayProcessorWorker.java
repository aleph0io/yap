package io.aleph0.yap.messaging.gcp;

import static java.util.Objects.requireNonNull;
import java.io.IOException;
import java.util.List;
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
public class PubsubRelayProcessorWorker<T> implements RelayProcessorWorker<T, T> {
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

  private final MessageExtractor<T> messageExtractor;

  public PubsubRelayProcessorWorker(PublisherFactory publisherFactory,
      MessageExtractor<T> messageExtractor) {
    this.publisherFactory = requireNonNull(publisherFactory, "publisherFactory");
    this.messageExtractor = requireNonNull(messageExtractor, "messageExtractor");
  }

  @Override
  public void process(Source<T> source, Sink<T> sink) throws IOException, InterruptedException {
    try {
      final Publisher publisher = publisherFactory.newPublisher();
      try {
        final AtomicReference<Exception> failureCause = new AtomicReference<>(null);
        for (T input = source.take(); input != null; input = source.take()) {
          final List<PubsubMessage> messages = messageExtractor.extractMessages(input);
          for (PubsubMessage message : messages) {
            // Throw the failure cause if we have one
            throwIfPresent(failureCause);

            // Publish the message to the PubSub
            ApiFuture<String> future = publisher.publish(message);

            submittedMetric.incrementAndGet();

            phaser.register();

            final T theinput = input;
            ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
              @Override
              public void onSuccess(String result) {
                try {
                  acknowledgedMetric.incrementAndGet();

                  phaser.arriveAndDeregister();

                  sink.put(theinput);
                } catch (Exception e) {
                  LOGGER.atError().addKeyValue("message", e.getMessage()).setCause(e)
                      .log("Failed to send acked state downstream. Failing task...");
                  failureCause.compareAndSet(null, e);
                }
              }

              @Override
              public void onFailure(Throwable t) {
                LOGGER.atError().addKeyValue("message", t.getMessage()).setCause(t)
                    .log("Message failed to publish. Interrupting event thread...");

                failureCause.compareAndSet(null, t instanceof Exception ? (Exception) t
                    : new Exception("Message failed to publish", t));

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
      throw e;
    } catch (Exception e) {
      LOGGER.atError().setCause(e).log("Error during message processing. Exiting...");
      if (e instanceof IOException x)
        throw x;
      if (e instanceof RuntimeException x)
        throw x;
      throw new IOException("Error during message processing", e);
    }
  }

  private <E extends Exception> void throwIfPresent(AtomicReference<E> failure) throws E {
    final E fc = failure.getAndSet(null);
    if (fc != null)
      throw fc;
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
