package io.aleph0.yap.core.transport.queue;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import io.aleph0.yap.core.transport.Channel;
import io.aleph0.yap.core.transport.Queue;
import io.aleph0.yap.core.transport.channel.DefaultChannel;

class DefaultQueueTest {

  @Test
  void givenFullQueue_whenTryPublish_thenReturnsFalse() {
    // Arrange
    int queueCapacity = 3;
    Channel<String> channel = new DefaultChannel<>();
    DefaultQueue.<String>builder().setCapacity(queueCapacity).build(List.of(channel));

    // Fill the queue to capacity
    for (int i = 0; i < queueCapacity; i++) {
      boolean result = channel.tryPublish("msg-" + i);
      assertThat(result).isTrue();
    }

    // Act
    boolean result = channel.tryPublish("one-too-many");

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  @Timeout(5)
  void givenFullQueue_whenPublish_thenBlocks() throws Exception {
    // Arrange
    int queueCapacity = 3;
    Channel<String> channel = new DefaultChannel<>();
    Queue<String> queue =
        DefaultQueue.<String>builder().setCapacity(queueCapacity).build(List.of(channel));

    // Fill the queue to capacity
    for (int i = 0; i < queueCapacity; i++)
      channel.tryPublish("msg-" + i);

    // Act & Assert
    Thread publishThread = new Thread(() -> {
      try {
        // This should block
        channel.publish("blocking-message");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    publishThread.start();

    publishThread.join(10);

    assertThat(publishThread.isAlive()).isTrue();

    assertThat(queue.tryReceive()).isNotNull();

    publishThread.join(10);

    assertThat(publishThread.isAlive()).isFalse();
  }

  @Test
  void givenEmptyQueue_whenTryReceive_thenNull() throws Exception {
    // Arrange
    Channel<String> channel = new DefaultChannel<>();
    Queue<String> queue = DefaultQueue.<String>builder().setCapacity(5).build(List.of(channel));

    // Act & Assert
    String message = queue.tryReceive();

    assertThat(message).isNull();
  }

  @Test
  @Timeout(5)
  void givenEmptyQueue_whenReceiveWithTimeoutWithoutMessage_thenWaitsAndThrowsTimeoutException()
      throws Exception {
    // Arrange
    Channel<String> channel = new DefaultChannel<>();
    Queue<String> queue = DefaultQueue.<String>builder().setCapacity(5).build(List.of(channel));

    // Act & Assert
    final Duration timeout = Duration.ofMillis(10L);
    final AtomicBoolean caught = new AtomicBoolean(false);
    Thread receiveThread = new Thread(() -> {
      try {
        // This should block
        queue.receive(timeout);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (TimeoutException e) {
        caught.set(true);
      }
    });

    final Instant now = Instant.now();

    receiveThread.start();
    receiveThread.join();

    final Instant then = Instant.now();

    assertThat(caught.get()).isTrue();
    assertThat(Duration.between(now, then)).isGreaterThanOrEqualTo(timeout);
  }

  @Test
  @Timeout(5)
  void givenEmptyQueue_whenReceiveWithTimeoutWithMessage_thenWaitsAndReceives() throws Exception {
    // Arrange
    Channel<String> channel = new DefaultChannel<>();
    Queue<String> queue = DefaultQueue.<String>builder().setCapacity(5).build(List.of(channel));

    // Act & Assert
    final Duration timeout = Duration.ofMillis(20L);
    final AtomicReference<String> received = new AtomicReference<>();
    final AtomicBoolean caught = new AtomicBoolean(false);
    Thread receiveThread = new Thread(() -> {
      try {
        // This should block
        received.set(queue.receive(timeout));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (TimeoutException e) {
        caught.set(true);
      }
    });

    final Instant now = Instant.now();

    receiveThread.start();

    receiveThread.join(10);

    channel.publish("test-message");

    receiveThread.join();

    final Instant then = Instant.now();

    assertThat(received.get()).isEqualTo("test-message");
    assertThat(caught.get()).isFalse();
    assertThat(Duration.between(now, then)).isLessThanOrEqualTo(timeout);
  }

  @Test
  @Timeout(5)
  void givenEmptyQueue_whenReceive_thenBlocks() throws Exception {
    // Arrange
    Channel<String> channel = new DefaultChannel<>();
    Queue<String> queue = DefaultQueue.<String>builder().setCapacity(5).build(List.of(channel));

    // Act & Assert
    final AtomicReference<String> received = new AtomicReference<>();
    Thread receiveThread = new Thread(() -> {
      try {
        // This should block
        received.set(queue.receive());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    receiveThread.start();

    Thread.sleep(10);

    assertThat(receiveThread.isAlive()).isTrue();

    channel.publish("test-message");

    receiveThread.join();

    assertThat(received.get()).isEqualTo("test-message");
  }

  @Test
  @Timeout(5)
  void givenBlockedPublish_whenChannelClosed_thenUnblocksAndThrows() throws Exception {
    // Arrange
    int queueCapacity = 3;
    Channel<String> channel = new DefaultChannel<>();
    Queue<String> queue =
        DefaultQueue.<String>builder().setCapacity(queueCapacity).build(List.of(channel));

    // Fill the queue to capacity
    for (int i = 0; i < queueCapacity; i++) {
      channel.tryPublish("msg-" + i);
    }

    // Act
    AtomicReference<Exception> publishException = new AtomicReference<>();

    Thread publishThread = new Thread(() -> {
      try {
        // This should block
        channel.publish("blocked-message");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        publishException.set(e);
      }
    });

    publishThread.start();

    Thread.sleep(10); // Give time for publish to block

    assertThat(publishThread.isAlive()).isTrue();

    // Close the channel to unblock with exception
    channel.close();

    publishThread.join();

    Queue.Metrics metrics = queue.flushMetrics();

    assertThat(publishException.get()).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");
    assertThat(metrics.stalls()).isEqualTo(1L);
    assertThat(metrics.produced()).isEqualTo(queueCapacity);
    assertThat(metrics.consumed()).isEqualTo(0L);
    assertThat(metrics.pending()).isEqualTo(queueCapacity);
    assertThat(metrics.waits()).isEqualTo(0L);
  }

  @Test
  @Timeout(5)
  void givenBlockedReceive_whenFullyDrained_thenUnblocksAndReturnsNull() throws Exception {
    // Arrange
    Channel<String> channel = new DefaultChannel<>();
    Queue<String> queue = DefaultQueue.<String>builder().setCapacity(5).build(List.of(channel));

    // Act
    AtomicReference<String> receivedMessage = new AtomicReference<>();

    Thread receiveThread = new Thread(() -> {
      try {
        // This should block
        receivedMessage.set(queue.receive());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    receiveThread.start();

    Thread.sleep(10); // Give time for receive to block

    assertThat(receiveThread.isAlive()).isTrue();

    // Close channel to drain the queue
    channel.close();

    receiveThread.join();

    Queue.Metrics metrics = queue.flushMetrics();

    assertThat(receivedMessage.get()).isNull();
    assertThat(queue.isDrained()).isTrue();
    assertThat(metrics.stalls()).isEqualTo(0L);
    assertThat(metrics.produced()).isEqualTo(0L);
    assertThat(metrics.consumed()).isEqualTo(0L);
    assertThat(metrics.pending()).isEqualTo(0L);
    assertThat(metrics.waits()).isEqualTo(1L);
  }
}
