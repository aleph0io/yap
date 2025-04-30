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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.messaging.core.RelayMetrics;

class TestRelayProcessorWorkerTest {

  // Test Source implementation
  private static class TestSource<T> implements Source<T> {
    private final BlockingQueue<T> queue = new LinkedBlockingQueue<>();

    public void add(T value) {
      queue.add(value);
    }

    @Override
    public T take() throws InterruptedException {
      return queue.poll();
    }

    @Override
    public T tryTake() {
      return queue.poll();
    }

    @Override
    public T take(Duration timeout) throws InterruptedException, TimeoutException {
      T result = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
      if (result == null)
        throw new TimeoutException();
      return result;
    }
  }

  // Test Sink implementation
  private static class TestSink<T> implements Sink<T> {
    private final BlockingQueue<T> receivedValues = new LinkedBlockingQueue<>();
    private final AtomicBoolean failOnPut = new AtomicBoolean(false);
    private final AtomicInteger putCount = new AtomicInteger(0);
    private final CountDownLatch receiveLatch = new CountDownLatch(1);

    @Override
    public void put(T value) throws InterruptedException {
      putCount.incrementAndGet();
      if (failOnPut.get())
        throw new RuntimeException("simulated sink failure");
      receivedValues.add(value);
      receiveLatch.countDown();
    }

    public void setFailOnPut(boolean fail) {
      failOnPut.set(fail);
    }

    public List<T> getReceivedValues() {
      List<T> result = new ArrayList<>();
      receivedValues.drainTo(result);
      return result;
    }

    public int size() {
      return receivedValues.size();
    }

    public void awaitReceive() throws InterruptedException {
      receiveLatch.await();
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

  // Test increasing-delay scheduler
  private static class IncreasingDelayScheduler implements Scheduler {
    private final AtomicInteger callCount = new AtomicInteger(0);

    @Override
    public Duration schedule() {
      int count = callCount.getAndIncrement();
      return Duration.ofMillis(count * 10); // 0ms, 10ms, 20ms...
    }

    public int getCallCount() {
      return callCount.get();
    }
  }

  @Test
  @Timeout(5)
  void testBasicMessageProcessing() throws Exception {
    // Setup with small delay for quick test execution
    Scheduler shortDelayScheduler = new FixedDelayScheduler(Duration.ofMillis(10));
    TestRelayProcessorWorker<String> worker = new TestRelayProcessorWorker<>(shortDelayScheduler);

    TestSource<String> source = new TestSource<>();
    TestSink<String> sink = new TestSink<>();

    // Add messages to source
    source.add("message1");
    source.add("message2");
    source.add("message3");

    // Process in a separate thread
    Thread processorThread = new Thread(() -> {
      try {
        worker.process(source, sink);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    try {
      processorThread.start();

      // Wait for messages to be processed (with delay)
      processorThread.join();

      // Verify messages were received in order
      List<String> receivedMessages = sink.getReceivedValues();
      assertThat(receivedMessages).containsExactly("message1", "message2", "message3");

      // Check metrics
      RelayMetrics metrics = worker.checkMetrics();
      assertThat(metrics.submitted()).isEqualTo(3);
      assertThat(metrics.acknowledged()).isEqualTo(3);
      assertThat(metrics.awaiting()).isEqualTo(0);

      // Test flush metrics
      RelayMetrics flushedMetrics = worker.flushMetrics();
      assertThat(flushedMetrics.submitted()).isEqualTo(3);
      assertThat(worker.checkMetrics().submitted()).isEqualTo(0);
    } finally {
      processorThread.interrupt();
      processorThread.join(1000);
    }
  }

  @Test
  @Timeout(5)
  void testInterruptionHandling() throws Exception {
    // Use a longer delay to ensure we can interrupt during processing
    Scheduler longDelayScheduler = new FixedDelayScheduler(Duration.ofMillis(5000));
    TestRelayProcessorWorker<String> worker = new TestRelayProcessorWorker<>(longDelayScheduler);

    TestSource<String> source = new TestSource<>();
    TestSink<String> sink = new TestSink<>();

    // Add messages to source
    source.add("message1");

    // Use latch to detect interruption
    final CountDownLatch interruptionLatch = new CountDownLatch(1);
    Thread processorThread = new Thread(() -> {
      try {
        worker.process(source, sink);
      } catch (InterruptedException e) {
        interruptionLatch.countDown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    try {
      processorThread.start();
      processorThread.interrupt();
      interruptionLatch.await();

      // Wait for scheduled tasks to complete
      processorThread.join(1000);

      // Check metrics - should show at least one message submitted
      RelayMetrics metrics = worker.checkMetrics();
      assertThat(metrics.submitted()).isGreaterThanOrEqualTo(1);
    } finally {
      if (processorThread.isAlive()) {
        processorThread.interrupt();
        processorThread.join(1000);
      }
    }
  }

  @Test
  void testSinkFailure() throws Exception {
    Scheduler immediateScheduler = new FixedDelayScheduler(Duration.ZERO);
    TestRelayProcessorWorker<String> worker = new TestRelayProcessorWorker<>(immediateScheduler);

    TestSource<String> source = new TestSource<>();
    TestSink<String> sink = new TestSink<>();

    // Set sink to fail on put
    sink.setFailOnPut(true);

    // Add a message to source
    source.add("message1");

    // Process and expect failure
    final AtomicReference<Throwable> processorFailureCause = new AtomicReference<>(null);
    Thread processorThread = new Thread(() -> {
      try {
        worker.process(source, sink);
      } catch (Exception e) {
        processorFailureCause.set(e);
      }
    });

    try {
      processorThread.start();
      processorThread.join();

      assertThat(processorFailureCause.get()).isExactlyInstanceOf(RuntimeException.class)
          .hasMessage("simulated sink failure");

      // Check metrics
      RelayMetrics metrics = worker.checkMetrics();
      assertThat(metrics.submitted()).isEqualTo(1);
      assertThat(metrics.acknowledged()).isEqualTo(0); // No acks because of failure
      assertThat(metrics.awaiting()).isEqualTo(1); // Still awaiting
    } finally {
      processorThread.interrupt();
      processorThread.join(1000);
    }
  }

  @Test
  void testNegativeDelay() {
    // Create a scheduler that returns negative delay
    Scheduler negativeDelayScheduler = () -> Duration.ofMillis(-10);
    TestRelayProcessorWorker<String> worker =
        new TestRelayProcessorWorker<>(negativeDelayScheduler);

    TestSource<String> source = new TestSource<>();
    TestSink<String> sink = new TestSink<>();

    // Add a message to source
    source.add("message1");

    // Process and expect IllegalArgumentException
    assertThatThrownBy(() -> worker.process(source, sink))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("scheduler returned negative delay");
  }

  @Test
  @Timeout(5)
  void testCustomSchedulerTiming() throws Exception {
    // Use increasing delay scheduler
    IncreasingDelayScheduler scheduler = new IncreasingDelayScheduler();
    TestRelayProcessorWorker<String> worker = new TestRelayProcessorWorker<>(scheduler);

    TestSource<String> source = new TestSource<>();
    TestSink<String> sink = new TestSink<>();

    // Add messages to source
    source.add("message1"); // Will be delivered immediately (0ms)
    source.add("message2"); // Will be delivered after 10ms
    source.add("message3"); // Will be delivered after 20ms

    // Process in a separate thread
    Thread processorThread = new Thread(() -> {
      try {
        worker.process(source, sink);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    try {
      processorThread.start();
      processorThread.join();

      // Check messages
      List<String> receivedMessages = sink.getReceivedValues();
      assertThat(receivedMessages).hasSize(3);

      // Verify scheduler was called correctly
      assertThat(scheduler.getCallCount()).isEqualTo(3);

      // Check metrics
      RelayMetrics metrics = worker.checkMetrics();
      assertThat(metrics.submitted()).isEqualTo(3);
      assertThat(metrics.acknowledged()).isEqualTo(3);
    } finally {
      processorThread.interrupt();
      processorThread.join(1000);
    }
  }

  @Test
  @Timeout(5)
  void testDefaultScheduler() throws Exception {
    // Test with default scheduler
    TestRelayProcessorWorker<String> worker = new TestRelayProcessorWorker<>();

    TestSource<String> source = new TestSource<>();
    TestSink<String> sink = new TestSink<>();

    source.add("message1");

    // Process in a separate thread
    Thread processorThread = new Thread(() -> {
      try {
        worker.process(source, sink);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    try {
      processorThread.start();
      processorThread.join();

      // Verify message was received
      assertThat(sink.size()).isEqualTo(1);

      // Check metrics
      RelayMetrics metrics = worker.checkMetrics();
      assertThat(metrics.submitted()).isEqualTo(1);
      assertThat(metrics.acknowledged()).isEqualTo(1);
    } finally {
      processorThread.interrupt();
      processorThread.join(1000);
    }
  }

  @Test
  void testNullParameter() {
    assertThatThrownBy(() -> new TestRelayProcessorWorker<>(null))
        .isInstanceOf(NullPointerException.class).hasMessage("scheduler");
  }
}
