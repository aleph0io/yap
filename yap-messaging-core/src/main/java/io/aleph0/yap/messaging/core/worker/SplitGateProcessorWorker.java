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
package io.aleph0.yap.messaging.core.worker;

import static java.util.Objects.requireNonNull;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.core.worker.MeasuredProcessorWorker;
import io.aleph0.yap.messaging.core.Acknowledgeable;
import io.aleph0.yap.messaging.core.Message;
import io.aleph0.yap.messaging.core.worker.SplitGateProcessorWorker.Metrics;

/**
 * A {@link ProcessorWorker} that implements a gating "split-join" pattern over messages:
 * <ul>
 * <li>Each input message is passed through a splitter function that produces zero or more output
 * messages.</li>
 * <li>If the splitter produces no messages, the input message (after mapping) is forwarded
 * immediately downstream.</li>
 * <li>If the splitter produces one or more messages, the input message is held back.</li>
 * <li>The split messages are sent downstream immediately and tracked for acknowledgement.</li>
 * <li>If all of the split messages complete successfully (i.e., are acked), then the input message
 * is forwarded downstream immediately after the last ack.</li>
 * <li>If any of the split messages fail (i.e., are nacked), then the input message is nacked
 * immediately, and any remaining split messages acknowledgments are ignored.</li>
 * </ul>
 *
 * <p>
 * This pattern is useful when downstream logic depends on all child messages being processed before
 * the parent can proceed, for example not acknowledging message until all its children have
 * themselves been acknowledged.
 * </p>
 * 
 * <p>
 * This class is loosely inspired by the Enterprise Integration Patterns "Splitter" and
 * "Aggregator". In the EIP framework, this pattern would be implemented using a stateful message
 * store that tracked all children messages a little more explicitly, perhaps using control messages
 * instead of data messages. This class simply tracks all child messages in memory, since workers
 * always runin in a single process.
 * </p>
 *
 * <p>
 * The {@code mapperFunction} is applied to the original message to produce the final representation
 * of the parent message that will be emitted after all child messages are complete. This class also
 * exposes a {@code gated} metric to indicate how many parent messages are currently waiting on
 * child acknowledgements.
 * </p>
 * 
 * <p>
 * The splitter messages' message IDs are derived from the parent message ID, so that they can be
 * easily be correlated with their parent. The original message's attributes are copied to the split
 * messages.
 * </p>
 *
 * @param <X> the type of the input message body
 * @param <Y> the type of the output message body
 */
public class SplitGateProcessorWorker<X, Y>
    implements MeasuredProcessorWorker<Message<X>, Message<Y>, Metrics> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplitGateProcessorWorker.class);

  public static record Metrics(int gated) {
    public Metrics {
      if (gated < 0)
        throw new IllegalArgumentException("gatedMetric must not be negative");
    }
  }

  private final AtomicInteger gatedMetric = new AtomicInteger(0);
  private final Function<X, Y> mapperFunction;
  private final Function<X, Collection<Y>> splitterFunction;

  public SplitGateProcessorWorker(Function<X, Y> mapperFunction,
      Function<X, Collection<Y>> splitterFunction) {
    this.mapperFunction = requireNonNull(mapperFunction, "mapperFunction");
    this.splitterFunction = requireNonNull(splitterFunction, "splitterFunction");
  }

  @Override
  public void process(Source<Message<X>> source, Sink<Message<Y>> sink)
      throws InterruptedException {
    for (Message<X> mi = source.take(); mi != null; mi = source.take()) {
      // Unpack our original message
      final Message<X> originalMessage = mi;
      final String originalId = originalMessage.id();
      final Map<String, String> originalAttributes = originalMessage.attributes();
      final X originalBody = mi.body();

      // Evaluate our split function to determine how many child messages we need to create. If
      // there are none, we can just pass the original message through. If there are some, we need
      // to track them and gate the original message until they are all acknowledged.
      final Collection<Y> splitBodies = splitterFunction.apply(originalBody);

      // Evaluate our mapper function to produce the final representation of the original message
      // that will be emitted after all child messages are complete. This is done regardless of
      // whether there are split messages or not, since the original message may need to be
      // transformed before being emitted.
      final Y mappedBody = mapperFunction.apply(originalBody);

      // Use the original message as the acknowledgeable for the mapped message, so that
      // acknowledging or nacking the mapped message will trigger the appropriate acknowledgment on
      // the original message. This allows us to ensure that the original message is only
      // acknowledged once all child messages have been processed.
      final Acknowledgeable mappedAcknowledgeable = originalMessage;

      // Create the mapped message using the newMappedMessage method, which can be overridden by
      // subclasses to customize the creation of mapped messages. The default implementation creates
      // a simple message with the mapped body and the same attributes as the original message.
      final Message<Y> mappedMessage = newMappedMessage(originalId, originalAttributes,
          originalBody, mappedBody, mappedAcknowledgeable);

      // Did we end up with any split messages? If not, we can just pass the mapped message through
      // immediately. If we did, we need to track them and gate the original message until they are
      // all acknowledged.
      if (splitBodies.isEmpty()) {
        // We do not have split messages, so we can just pass the mapped message through
        // immediately.
        sink.put(mappedMessage);
      } else {
        // We have split messages, so we need to track them and gate the original message until they
        // are all acknowledged. We increment the gated metric to indicate that we have a message
        // that is waiting on child acknowledgements.
        gatedMetric.incrementAndGet();

        // Set up our acknowledgeable to track the acknowledgement of the split messages
        final AtomicInteger counter = new AtomicInteger(splitBodies.size());
        final Acknowledgeable splitAcknowledgeable = new Acknowledgeable() {
          @Override
          public void ack(AcknowledgementListener listener) {
            final int count = counter.decrementAndGet();
            try {
              listener.onSuccess();
            } finally {
              if (count == 0) {
                gatedMetric.decrementAndGet();
                try {
                  sink.put(mappedMessage);
                } catch (InterruptedException e) {
                  LOGGER.atError().setCause(e).addKeyValue("messageId", originalId)
                      .log("Failed to put gatedMetric message due to interruption");
                  Thread.currentThread().interrupt();
                }
              }
            }
          }

          @Override
          public void nack(AcknowledgementListener listener) {
            final int count = counter.getAndSet(-1);
            try {
              listener.onSuccess();
            } finally {
              if (count > 0) {
                // We nack the original message straightaway
                mappedMessage.nack(new AcknowledgementListener() {
                  @Override
                  public void onSuccess() {
                    gatedMetric.decrementAndGet();
                    LOGGER.atDebug().addKeyValue("messageId", originalId)
                        .log("Successfully nacked gatedMetric message");
                  }

                  @Override
                  public void onFailure(Throwable t) {
                    gatedMetric.decrementAndGet();
                    LOGGER.atWarn().setCause(t).addKeyValue("messageId", originalId)
                        .log("Failed to nack gatedMetric message");
                  }
                });
              }
            }
          }
        };

        // Create and emit the split messages
        int index = 0;
        final Iterator<Y> iterator = splitBodies.iterator();
        while (iterator.hasNext()) {
          final Y splitBody = iterator.next();
          final Message<Y> splitMessage = newSplitMessage(originalId, originalAttributes,
              originalBody, splitBody, index, splitAcknowledgeable);
          sink.put(splitMessage);
          index = index + 1;
        }
      }
    }
  }

  /**
   * Creates a new mapped message based on the original message's ID and attributes, with the
   * provided mapped body. The acknowledgment behavior is defined such that acknowledging or nacking
   * the mapped message will trigger the appropriate acknowledgment on the original message.
   * 
   * <p>
   * This method can be overridden by subclasses to customize the creation of mapped messages, for
   * example to add additional attributes or to change the acknowledgment behavior. The default
   * implementation creates a simple message with the mapped body and the same attributes as the
   * original message.
   * 
   * @param originalId the ID of the original message, which can be used as a base for the mapped
   *        message ID
   * @param originalAttributes the attributes of the original message, which can be used as a base
   *        for the mapped message attributes
   * @param originalBody the body of the original message, which can be used for reference or
   *        mapping in the mapped message
   * @param mappedBody the body of the mapped message, which is the result of the mapping operation
   *        and must be used as the body of the new mapped message
   * @param acknwoledgeable The object to use to ack or nack the new message
   * @return the new message
   */
  protected Message<Y> newMappedMessage(String originalId, Map<String, String> originalAttributes,
      X originalBody, Y mappedBody, Acknowledgeable acknwoledgeable) {
    return new Message<>() {
      private final AtomicBoolean acked = new AtomicBoolean(false);

      @Override
      public String id() {
        return originalId;
      }

      @Override
      public Map<String, String> attributes() {
        return originalAttributes;
      }

      @Override
      public Y body() {
        return mappedBody;
      }

      @Override
      public void ack(AcknowledgementListener listener) {
        final boolean updated = acked.compareAndSet(false, true);
        if (updated) {
          acknwoledgeable.ack(listener);
        } else {
          LOGGER.atWarn().addKeyValue("messageId", originalId).addArgument(originalId)
              .log("Attempted to ack a message that has already been acknowledged: {}");
        }
      }

      @Override
      public void nack(AcknowledgementListener listener) {
        final boolean updated = acked.compareAndSet(false, true);
        if (updated) {
          acknwoledgeable.nack(listener);
        } else {
          LOGGER.atWarn().addKeyValue("messageId", originalId).addArgument(originalId)
              .log("Attempted to nack a message that has already been acknowledged: {}");
        }
      }
    };
  }

  /**
   * Creates a new split message based on the original message's ID and attributes, with the
   * provided split body. The split message will have a unique ID derived from the original
   * message's ID and the index of the split. The acknowledgment behavior is defined such that
   * acknowledging or nacking the split message will trigger the appropriate acknowledgment on the
   * original message once all splits are processed.
   * 
   * <p>
   * This method can be overriden by subclasses to customize the creation of split messages, for
   * example to add additional attributes or to change the acknowledgment behavior. The default
   * implementation creates a simple message with the split body and the same attributes as the
   * original message, and uses a unique ID based on the original message ID and the index of the
   * split.
   * 
   * @param originalId the ID of the original message, which can be used as a base for the split
   *        message ID
   * @param originalAttributes the attributes of the original message, which can be used as a base
   *        for the split message attributes
   * @param originalBody the body of the original message, which can be used for reference or
   *        mapping in the split message
   * @param splitBody the body of the split message, which is the result of the splitting operation
   *        and must be used as the body of the new split message
   * @param index the zero-based index of the split message, which can be used to create a unique ID
   *        for the split message. The first split message will have an index of 0, the second will
   *        have an index of 1, and so on.
   * @param acknowledge The object to use to ack or nack the new message
   * @return the new message
   */
  protected Message<Y> newSplitMessage(String originalId, Map<String, String> originalAttributes,
      X originalBody, Y splitBody, int index, Acknowledgeable acknowledge) {
    final String id = originalId + "-" + index;
    final Map<String, String> attributes = originalAttributes;
    return new Message<>() {
      private final AtomicBoolean acked = new AtomicBoolean(false);

      @Override
      public String id() {
        return id;
      }

      @Override
      public Map<String, String> attributes() {
        return attributes;
      }

      @Override
      public Y body() {
        return splitBody;
      }

      @Override
      public void ack(AcknowledgementListener listener) {
        final boolean updated = acked.compareAndSet(false, true);
        if (updated) {
          acknowledge.ack(listener);
        } else {
          LOGGER.atWarn().addKeyValue("messageId", id).addArgument(id)
              .log("Attempted to ack a message that has already been acknowledged: {}");
        }
      }

      @Override
      public void nack(AcknowledgementListener listener) {
        final boolean updated = acked.compareAndSet(false, true);
        if (updated) {
          acknowledge.nack(listener);
        } else {
          LOGGER.atWarn().addKeyValue("messageId", id).addArgument(id)
              .log("Attempted to nack a message that has already been acknowledged: {}");
        }
      }
    };
  }

  @Override
  public Metrics checkMetrics() {
    final int gated = gatedMetric.get();
    return new Metrics(gated);
  }

  @Override
  public Metrics flushMetrics() {
    return checkMetrics();
  }
}
