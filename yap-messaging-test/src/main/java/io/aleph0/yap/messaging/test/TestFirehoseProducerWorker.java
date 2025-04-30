/*-
 * =================================LICENSE_START==================================
 * yap-messaging-test
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
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.messaging.core.FirehoseMetrics;
import io.aleph0.yap.messaging.core.FirehoseProducerWorker;
import io.aleph0.yap.messaging.core.Message;

/**
 * A {@link FirehoseProducerWorker} that produces messages using a {@link Supplier} according to a
 * {@link Scheduler}. This is useful for testing and debugging purposes, as it allows you to
 * simulate the behavior of a firehose producer without actually sending the messages anywhere.
 * 
 * <p>
 * The firehose will produce messages at a rate determined by the {@link Scheduler}. The scheduler
 * will determine the delay between each message produced. The firehose will produce messages until
 * the given message supplier returns {@code null}.
 * 
 * @param <T> the type of the message to produce
 */
public class TestFirehoseProducerWorker<T> implements FirehoseProducerWorker<Message<T>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestFirehoseProducerWorker.class);

  private final AtomicLong receivedMetric = new AtomicLong(0);

  private final Scheduler scheduler;
  private final Supplier<Message<T>> messageSupplier;

  public TestFirehoseProducerWorker(Supplier<Message<T>> messageSupplier) {
    this(Scheduler.defaultScheduler(), messageSupplier);
  }

  public TestFirehoseProducerWorker(Scheduler scheduler, Supplier<Message<T>> messageSupplier) {
    this.scheduler = requireNonNull(scheduler, "scheduler");
    this.messageSupplier = requireNonNull(messageSupplier, "messageSupplier");
  }

  public void produce(Sink<Message<T>> sink) throws InterruptedException {
    try {
      for (Message<T> message = messageSupplier.get(); message != null; message =
          messageSupplier.get()) {
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          throw new InterruptedException();
        }

        final Duration delay = scheduler.schedule();

        Thread.sleep(delay.toMillis());

        receivedMetric.incrementAndGet();

        sink.put(message);
      }
    } catch (InterruptedException e) {
      LOGGER.atError().setCause(e).log("Test firehose interrupted. Propagating...");
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    }
  }

  @Override
  public FirehoseMetrics checkMetrics() {
    final long received = receivedMetric.get();
    return new FirehoseMetrics(received);
  }

  @Override
  public FirehoseMetrics flushMetrics() {
    FirehoseMetrics result = checkMetrics();
    receivedMetric.set(0);
    return result;
  }
}
