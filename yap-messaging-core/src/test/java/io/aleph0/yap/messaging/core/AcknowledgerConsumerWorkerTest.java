package io.aleph0.yap.messaging.core;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import io.aleph0.yap.core.Source;

public class AcknowledgerConsumerWorkerTest {

  // Test message implementation
  static class TestAcknowledgeable implements Acknowledgeable {
    private final CountDownLatch ackLatch = new CountDownLatch(1);
    private final AtomicReference<AcknowledgementListener> listener = new AtomicReference<>();
    private final String id;

    public TestAcknowledgeable(String id) {
      this.id = id;
    }

    @Override
    public void ack(AcknowledgementListener listener) {
      this.listener.set(listener);
      ackLatch.countDown();
    }

    @Override
    public void nack(AcknowledgementListener listener) {
      throw new UnsupportedOperationException("nack not under test");
    }

    public void completeAckSuccess() {
      AcknowledgementListener l = listener.get();
      if (l != null) {
        l.onSuccess();
      }
    }

    public void completeAckFailure(Throwable cause) {
      AcknowledgementListener l = listener.get();
      if (l != null) {
        l.onFailure(cause);
      }
    }

    public void waitForAck(long timeout, TimeUnit unit) throws InterruptedException {
      ackLatch.await(timeout, unit);
    }

    @Override
    public String toString() {
      return "TestAcknowledgeable[" + id + "]";
    }
  }

  // Custom source implementation for controlled testing
  static class TestSource implements Source<TestAcknowledgeable>, AutoCloseable {
    private final BlockingQueue<TestAcknowledgeable> queue = new LinkedBlockingQueue<>();
    private final CountDownLatch latch = new CountDownLatch(1);

    public TestSource(TestAcknowledgeable... messages) {
      this(List.of(messages));
    }

    public TestSource(List<TestAcknowledgeable> messages) {
      for (TestAcknowledgeable message : messages)
        queue.offer(message);
    }

    @Override
    public TestAcknowledgeable take() throws InterruptedException {
      TestAcknowledgeable result = queue.poll();
      if (result != null)
        return result;

      latch.await();

      return null;
    }

    @Override
    public TestAcknowledgeable tryTake() {
      return queue.poll();
    }

    @Override
    public TestAcknowledgeable take(Duration timeout)
        throws InterruptedException, TimeoutException {
      try {
        TestAcknowledgeable result = queue.poll();
        if (result != null)
          return result;

        latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);

        throw new TimeoutException("Timed out waiting for message");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      }
    }

    public void close() {
      latch.countDown();
    }
  }

  @Test
  void testSuccessfulAcknowledgment() throws Exception {
    // Create test messages
    TestAcknowledgeable msg1 = new TestAcknowledgeable("1");
    TestAcknowledgeable msg2 = new TestAcknowledgeable("2");
    TestAcknowledgeable msg3 = new TestAcknowledgeable("3");

    // Setup
    TestSource source = new TestSource(msg1, msg2, msg3);
    AcknowledgerConsumerWorker<TestAcknowledgeable> worker = new AcknowledgerConsumerWorker<>();

    // Start worker in separate thread
    Thread workerThread = new Thread(() -> {
      try {
        worker.consume(source);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    try {
      workerThread.start();

      // Add messages one by one
      msg1.waitForAck(1, TimeUnit.SECONDS);
      msg1.completeAckSuccess();

      msg2.waitForAck(1, TimeUnit.SECONDS);
      msg2.completeAckSuccess();

      msg3.waitForAck(1, TimeUnit.SECONDS);
      msg3.completeAckSuccess();

      source.close();

      workerThread.join();

      // Check metrics
      AcknowledgerMetrics metrics = worker.checkMetrics();
      assertThat(metrics.acknowledged()).isEqualTo(3);
      assertThat(metrics.retired()).isEqualTo(3);
      assertThat(metrics.retiredSuccess()).isEqualTo(3);
      assertThat(metrics.retiredFailure()).isEqualTo(0);
      assertThat(metrics.awaiting()).isEqualTo(0);

      // Test flush metrics
      AcknowledgerMetrics flushedMetrics = worker.flushMetrics();
      assertThat(flushedMetrics.acknowledged()).isEqualTo(3);
      assertThat(worker.checkMetrics().acknowledged()).isEqualTo(0);
    } finally {
      workerThread.interrupt();
      workerThread.join(1000);
    }
  }

  @Test
  @Timeout(5)
  void testFailureWithDefaultHandler() throws Exception {
    TestAcknowledgeable msg1 = new TestAcknowledgeable("1");
    TestAcknowledgeable msg2 = new TestAcknowledgeable("2");

    // Setup
    TestSource source = new TestSource(msg1, msg2);
    AcknowledgerConsumerWorker<TestAcknowledgeable> worker = new AcknowledgerConsumerWorker<>();

    // Use CountDownLatch to wait for expected exception
    AtomicReference<Throwable> workerFailureCause = new AtomicReference<>();
    Thread workerThread = new Thread(() -> {
      try {
        worker.consume(source);
      } catch (Exception e) {
        workerFailureCause.set(e);
      }
    });

    try {
      workerThread.start();

      // Process one message successfully
      msg1.waitForAck(1, TimeUnit.SECONDS);
      msg1.completeAckSuccess();

      // Process second message with failure
      msg2.waitForAck(1, TimeUnit.SECONDS);
      msg2.completeAckFailure(new RuntimeException("simulated failure"));

      source.close();

      // Wait for worker to fail
      workerThread.join();

      assertThat(workerFailureCause.get()).isInstanceOf(RuntimeException.class)
          .withFailMessage("simulated failure");

      // Check metrics
      AcknowledgerMetrics metrics = worker.checkMetrics();
      assertThat(metrics.acknowledged()).isEqualTo(2);
      assertThat(metrics.retired()).isEqualTo(2);
      assertThat(metrics.retiredSuccess()).isEqualTo(1);
      assertThat(metrics.retiredFailure()).isEqualTo(1);
      assertThat(metrics.awaiting()).isEqualTo(0);
    } finally {
      workerThread.interrupt();
      workerThread.join();
    }
  }

  @Test
  @Timeout(5)
  void testFailureWithCustomHandler() throws Exception {
    // Setup with custom handler that ignores failures
    TestAcknowledgeable msg1 = new TestAcknowledgeable("1");
    TestAcknowledgeable msg2 = new TestAcknowledgeable("2");

    TestSource source = new TestSource(msg1, msg2);
    AcknowledgerConsumerWorker<TestAcknowledgeable> worker =
        new AcknowledgerConsumerWorker<>(cause -> {
          System.err.println("Ignoring failure: " + cause.getMessage());
        });

    Thread workerThread = new Thread(() -> {
      try {
        worker.consume(source);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    try {
      workerThread.start();

      // Process one message successfully
      msg1.waitForAck(1, TimeUnit.SECONDS);
      msg1.completeAckSuccess();

      // Process second message with failure (but handler ignores it)
      msg2.waitForAck(1, TimeUnit.SECONDS);
      RuntimeException testException = new RuntimeException("Ignored failure");
      msg2.completeAckFailure(testException);

      source.close();

      // Give time for processing to complete
      workerThread.join();

      // Check metrics - should show one success and one failure, but no exceptions thrown
      AcknowledgerMetrics metrics = worker.checkMetrics();
      assertThat(metrics.acknowledged()).isEqualTo(2);
      assertThat(metrics.retired()).isEqualTo(2);
      assertThat(metrics.retiredSuccess()).isEqualTo(1);
      assertThat(metrics.retiredFailure()).isEqualTo(1);
      assertThat(metrics.awaiting()).isEqualTo(0);
    } finally {
      workerThread.interrupt();
      workerThread.join();
    }
  }

  @Test
  @Timeout(5)
  void testInterruptionHandling() throws Exception {
    TestAcknowledgeable msg = new TestAcknowledgeable("1");

    // Setup
    TestSource source = new TestSource(msg);
    AcknowledgerConsumerWorker<TestAcknowledgeable> worker = new AcknowledgerConsumerWorker<>();

    final AtomicReference<Throwable> workerFailureCause = new AtomicReference<>();
    Thread workerThread = new Thread(() -> {
      try {
        worker.consume(source);
      } catch (InterruptedException e) {
        workerFailureCause.set(e);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    try {
      workerThread.start();

      // Add a message but don't complete the ack yet
      assertThat(msg.ackLatch.await(1, TimeUnit.SECONDS)).isTrue();

      // Interrupt the worker
      workerThread.interrupt();

      // Now complete the ack
      msg.completeAckSuccess();

      source.close();

      workerThread.join();

      assertThat(workerFailureCause.get()).isInstanceOf(InterruptedException.class);

      // Check metrics
      AcknowledgerMetrics metrics = worker.checkMetrics();
      assertThat(metrics.acknowledged()).isEqualTo(1);
      assertThat(metrics.retired()).isEqualTo(1);
      assertThat(metrics.retiredSuccess()).isEqualTo(1);
      assertThat(metrics.awaiting()).isEqualTo(0);
    } finally {
      workerThread.interrupt();
      workerThread.join();
    }
  }

  @Test
  @Timeout(5)
  void testAckFailureWithInterruptedException() throws Exception {
    TestAcknowledgeable msg = new TestAcknowledgeable("1");

    // Setup
    TestSource source = new TestSource(msg);
    AcknowledgerConsumerWorker<TestAcknowledgeable> worker = new AcknowledgerConsumerWorker<>();

    final AtomicReference<Throwable> workerFailureCause = new AtomicReference<>();
    Thread workerThread = new Thread(() -> {
      try {
        worker.consume(source);
      } catch (InterruptedException e) {
        workerFailureCause.set(e);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        System.err.println("Worker thread finished");
      }
    });

    try {
      workerThread.start();

      msg.waitForAck(1, TimeUnit.SECONDS);

      // Fail with InterruptedException. We have to do this in another thread or it actually
      // interrupts the current thread!
      final Thread failerThread = new Thread(() -> {
        msg.completeAckFailure(new InterruptedException());
      });
      failerThread.start();
      failerThread.join();

      source.close();

      workerThread.join();

      assertThat(workerFailureCause.get()).isInstanceOf(InterruptedException.class);

      // Check metrics
      AcknowledgerMetrics metrics = worker.checkMetrics();
      assertThat(metrics.acknowledged()).isEqualTo(1);
      assertThat(metrics.retired()).isEqualTo(1);
      assertThat(metrics.retiredFailure()).isEqualTo(1);
    } finally

    {
      workerThread.interrupt();
      workerThread.join();
    }
  }

  @Test
  @Timeout(5)
  void testAckFailureWithError() throws Exception {
    TestAcknowledgeable msg = new TestAcknowledgeable("1");

    // Setup
    TestSource source = new TestSource(msg);
    AcknowledgerConsumerWorker<TestAcknowledgeable> worker = new AcknowledgerConsumerWorker<>();

    final AtomicReference<Throwable> workerFailureCause = new AtomicReference<>();
    Thread workerThread = new Thread(() -> {
      try {
        worker.consume(source);
      } catch (Exception e) {
        e.printStackTrace();
      } catch (Error e) {
        workerFailureCause.set(e);
      }
    });

    try {
      workerThread.start();

      msg.waitForAck(1, TimeUnit.SECONDS);

      // Fail with Error (should not be intercepted by handler)
      msg.completeAckFailure(new AssertionError("simulated error"));

      source.close();

      workerThread.join();

      assertThat(workerFailureCause.get()).isInstanceOf(AssertionError.class)
          .hasMessage("simulated error");
    } finally {
      workerThread.interrupt();
      workerThread.join();
    }
  }
}
