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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.messaging.core.FirehoseMetrics;
import io.aleph0.yap.messaging.core.Message;

class TestFirehoseProducerWorkerTest {

  // Test Message implementation
  private static class TestMessage<T> implements Message<T> {
    private final T value;
    private final String id;

    public TestMessage(String id, T value) {
      this.id = id;
      this.value = value;
    }

    @Override
    public T body() {
      return value;
    }

    @Override
    public String toString() {
      return "TestMessage[" + id + "]";
    }

    @Override
    public void ack(AcknowledgementListener listener) {
      throw new UnsupportedOperationException("ack not under test");
    }

    @Override
    public void nack(AcknowledgementListener listener) {
      throw new UnsupportedOperationException("nack not under test");
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public Map<String, String> attributes() {
      return Map.of();
    }
  }

  // Test Sink implementation
  private static class TestSink<T> implements Sink<T> {
    private final BlockingQueue<T> receivedMessages = new LinkedBlockingQueue<>();

    @Override
    public void put(T message) throws InterruptedException {
      receivedMessages.add(message);
    }

    public List<T> getReceivedMessages() {
      List<T> result = new ArrayList<>();
      receivedMessages.drainTo(result);
      return result;
    }

    public T take(long timeout, TimeUnit unit) throws InterruptedException {
      return receivedMessages.poll(timeout, unit);
    }

    public int size() {
      return receivedMessages.size();
    }
  }

  // Test fixed-delay scheduler
  private static class FixedDelayScheduler implements Scheduler {
    private final Duration delay;

    public FixedDelayScheduler(Duration delay) {
      this.delay = delay;
    }

    @Override
    public Duration schedule() {
      return delay;
    }
  }

  @Test
  void testBasicMessageProduction() throws Exception {
    // Setup with no delay for fast test execution
    Scheduler noDelayScheduler = new FixedDelayScheduler(Duration.ZERO);

    // Create 5 test messages
    AtomicInteger counter = new AtomicInteger(0);
    Supplier<Message<String>> messageSupplier = () -> {
      int i = counter.getAndIncrement();
      return i < 5 ? new TestMessage<>("msg-" + i, "content-" + i) : null;
    };

    // Create worker and sink
    TestFirehoseProducerWorker<String> worker =
        new TestFirehoseProducerWorker<>(noDelayScheduler, messageSupplier);
    TestSink<Message<String>> sink = new TestSink<>();

    // Run worker in separate thread
    Thread workerThread = new Thread(() -> {
      try {
        worker.produce(sink);
      } catch (InterruptedException e) {
        // Expected in test
      }
    });

    try {
      workerThread.start();

      // Wait for worker to complete (should finish when supplier returns null)
      workerThread.join(2000);
      assertThat(workerThread.isAlive()).isFalse();

      // Check messages
      List<Message<String>> receivedMessages = sink.getReceivedMessages();
      assertThat(receivedMessages).hasSize(5);

      // Verify message contents
      for (int i = 0; i < 5; i++) {
        Message<String> msg = receivedMessages.get(i);
        assertThat(msg.body()).isEqualTo("content-" + i);
      }

      // Check metrics
      FirehoseMetrics metrics = worker.checkMetrics();
      assertThat(metrics.received()).isEqualTo(5);

      // Test flush metrics
      FirehoseMetrics flushedMetrics = worker.flushMetrics();
      assertThat(flushedMetrics.received()).isEqualTo(5);
      assertThat(worker.checkMetrics().received()).isEqualTo(0);
    } finally {
      if (workerThread.isAlive()) {
        workerThread.interrupt();
        workerThread.join(1000);
      }
    }
  }

  @Test
  void testInterruptionHandling() throws Exception {
    // Use a long delay to ensure we can interrupt before completion
    Scheduler slowScheduler = new FixedDelayScheduler(Duration.ofMillis(500));

    // Create an infinite message supply
    AtomicInteger counter = new AtomicInteger(0);
    Supplier<Message<String>> infiniteSupplier = () -> {
      int i = counter.getAndIncrement();
      return new TestMessage<>("msg-" + i, "content-" + i);
    };

    TestFirehoseProducerWorker<String> worker =
        new TestFirehoseProducerWorker<>(slowScheduler, infiniteSupplier);
    TestSink<Message<String>> sink = new TestSink<>();

    // Use a latch to detect interruption
    CountDownLatch interruptionLatch = new CountDownLatch(1);

    Thread workerThread = new Thread(() -> {
      try {
        worker.produce(sink);
      } catch (InterruptedException e) {
        interruptionLatch.countDown();
      }
    });

    try {
      workerThread.start();

      // Wait for at least one message
      Message<String> firstMsg = sink.take(2, TimeUnit.SECONDS);
      assertThat(firstMsg).isNotNull();

      // Interrupt the worker
      workerThread.interrupt();

      // Verify worker was interrupted
      assertThat(interruptionLatch.await(2, TimeUnit.SECONDS)).isTrue();

      // Check metrics
      FirehoseMetrics metrics = worker.checkMetrics();
      assertThat(metrics.received()).isGreaterThanOrEqualTo(1);
    } finally {
      if (workerThread.isAlive()) {
        workerThread.interrupt();
        workerThread.join(1000);
      }
    }
  }

  @Test
  void testCustomScheduler() throws Exception {
    // Create a test scheduler that increases delay with each call
    AtomicInteger schedulerCallCount = new AtomicInteger(0);
    Scheduler increasingDelayScheduler = () -> {
      int callCount = schedulerCallCount.getAndIncrement();
      return Duration.ofMillis(callCount * 10); // 0ms, 10ms, 20ms, 30ms...
    };

    // Create 3 messages
    AtomicInteger counter = new AtomicInteger(0);
    Supplier<Message<String>> messageSupplier = () -> {
      int i = counter.getAndIncrement();
      return i < 3 ? new TestMessage<>("msg-" + i, "content-" + i) : null;
    };

    TestFirehoseProducerWorker<String> worker =
        new TestFirehoseProducerWorker<>(increasingDelayScheduler, messageSupplier);
    TestSink<Message<String>> sink = new TestSink<>();

    // Run worker and measure time
    long startTime = System.currentTimeMillis();
    worker.produce(sink);
    long endTime = System.currentTimeMillis();

    // Check messages were received
    assertThat(sink.size()).isEqualTo(3);

    // Verify scheduler was called correctly
    assertThat(schedulerCallCount.get()).isEqualTo(3);

    // Time should be at least the sum of delays (0 + 10 + 20 = 30ms)
    // But allow some flexibility for test environment
    assertThat(endTime - startTime).isGreaterThanOrEqualTo(20);

    // Check metrics
    assertThat(worker.checkMetrics().received()).isEqualTo(3);
  }

  @Test
  void testDefaultScheduler() throws Exception {
    // Test with default scheduler
    AtomicInteger counter = new AtomicInteger(0);
    Supplier<Message<String>> messageSupplier = () -> {
      int i = counter.getAndIncrement();
      return i < 1 ? new TestMessage<>("msg", "test") : null;
    };

    TestFirehoseProducerWorker<String> worker = new TestFirehoseProducerWorker<>(messageSupplier);
    TestSink<Message<String>> sink = new TestSink<>();

    worker.produce(sink);

    assertThat(sink.size()).isEqualTo(1);
    assertThat(worker.checkMetrics().received()).isEqualTo(1);
  }

  @Test
  void testNullChecks() {
    assertThatThrownBy(() -> new TestFirehoseProducerWorker<>(null, () -> null))
        .isInstanceOf(NullPointerException.class).hasMessage("scheduler");

    assertThatThrownBy(() -> new TestFirehoseProducerWorker<>(Scheduler.defaultScheduler(), null))
        .isInstanceOf(NullPointerException.class).hasMessage("messageSupplier");
  }
}
