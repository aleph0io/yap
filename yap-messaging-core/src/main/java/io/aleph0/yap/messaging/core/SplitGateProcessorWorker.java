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
import io.aleph0.yap.messaging.core.SplitGateProcessorWorker.Metrics;

/**
 * A {@link ProcessorWorker} that implements a gating "split-join" pattern over messages:
 * <ul>
 * <li>Each input message is passed through a {@code splitterFunction} that produces zero or more
 * output messages.</li>
 * <li>If the splitter produces no messages, the input message (after mapping) is forwarded
 * immediately downstream.</li>
 * <li>If the splitter produces one or more messages, the input message is held back.</li>
 * <li>The split messages are sent downstream immediately and tracked for acknowledgement.</li>
 * <li>Once <em>all</em> split messages are acknowledged (acked or nacked), the original message
 * (after mapping) is released downstream.</li>
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
    for (Message<X> m = source.take(); m != null; m = source.take()) {
      final Message<X> originalMessage = m;
      final X originalBody = m.body();
      final Collection<Y> splitBodies = splitterFunction.apply(originalBody);
      final Message<Y> mappedMessage = mappedMessage(originalMessage);
      if (splitBodies.isEmpty()) {
        sink.put(mappedMessage);
      } else {
        gatedMetric.incrementAndGet();
        int index = 0;
        final Iterator<Y> iterator = splitBodies.iterator();
        final AtomicInteger counter = new AtomicInteger(splitBodies.size());
        while (iterator.hasNext()) {
          final Message<Y> splitMessage =
              splitMessage(mappedMessage, counter, index, iterator.next(), sink);
          sink.put(splitMessage);
          index = index + 1;
        }
      }
    }
  }

  /* default */ Message<Y> mappedMessage(Message<X> originalMessage) {
    final Y mappedBody = mapperFunction.apply(originalMessage.body());
    return new Message<>() {
      @Override
      public void ack(AcknowledgementListener listener) {
        originalMessage.ack(listener);
      }

      @Override
      public void nack(AcknowledgementListener listener) {
        originalMessage.nack(listener);
      }

      @Override
      public String id() {
        return originalMessage.id();
      }

      @Override
      public Map<String, String> attributes() {
        return originalMessage.attributes();
      }

      @Override
      public Y body() {
        return mappedBody;
      }
    };
  }

  /* default */ Message<Y> splitMessage(Message<Y> mappedMessage, AtomicInteger counter, int index,
      Y splitBody, Sink<Message<Y>> sink) {
    final String id = mappedMessage.id() + "-" + index;
    return new Message<>() {
      private final AtomicBoolean acked = new AtomicBoolean(false);

      @Override
      public void ack(AcknowledgementListener listener) {
        final boolean updated = acked.compareAndSet(false, true);
        if (updated) {
          final int count = counter.decrementAndGet();
          try {
            listener.onSuccess();
          } finally {
            if (count == 0) {
              gatedMetric.decrementAndGet();
              try {
                sink.put(mappedMessage);
              } catch (InterruptedException e) {
                LOGGER.atError().setCause(e).addKeyValue("messageId", id)
                    .log("Failed to put gatedMetric message due to interruption");
                Thread.currentThread().interrupt();
              }
            }
          }
        }
      }

      @Override
      public void nack(AcknowledgementListener listener) {
        final boolean updated = acked.compareAndSet(false, true);
        if (updated) {
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
                  LOGGER.atDebug().addKeyValue("messageId", id)
                      .log("Successfully nacked gatedMetric message");
                }

                @Override
                public void onFailure(Throwable t) {
                  gatedMetric.decrementAndGet();
                  LOGGER.atWarn().setCause(t).addKeyValue("messageId", id)
                      .log("Failed to nack gatedMetric message");
                }
              });
            }
          }
        }
      }

      @Override
      public String id() {
        return id;
      }

      @Override
      public Map<String, String> attributes() {
        return mappedMessage.attributes();
      }

      @Override
      public Y body() {
        return splitBody;
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
