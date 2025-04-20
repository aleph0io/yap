/*-
 * =================================LICENSE_START==================================
 * yap-core
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
package io.aleph0.yap.core.transport.topic;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import io.aleph0.yap.core.transport.Channel;
import io.aleph0.yap.core.transport.Topic;

class DefaultTopicTest {

  @Test
  void givenTopic_whenPublishMessage_thenAllChannelsReceiveIt() {
    // Arrange
    TestChannel<String> channel1 = new TestChannel<>();
    TestChannel<String> channel2 = new TestChannel<>();
    List<Channel<String>> channels = List.of(channel1, channel2);

    // Act
    try (Topic<String> topic = new DefaultTopic<>(channels)) {
      try {
        topic.publish("test message");
      } catch (InterruptedException e) {
        fail("Unexpected interruption");
      }
    }

    // Assert
    assertThat(channel1.receivedMessages).containsExactly("test message");
    assertThat(channel2.receivedMessages).containsExactly("test message");
  }

  @Test
  void givenTopic_whenMultipleMessagesPublished_thenAllChannelsReceiveAllMessages() {
    // Arrange
    TestChannel<String> channel1 = new TestChannel<>();
    TestChannel<String> channel2 = new TestChannel<>();
    List<Channel<String>> channels = List.of(channel1, channel2);

    // Act
    try (Topic<String> topic = new DefaultTopic<>(channels)) {
      topic.publish("message 1");
      topic.publish("message 2");
      topic.publish("message 3");
    } catch (InterruptedException e) {
      fail("Unexpected interruption");
    }

    // Assert
    assertThat(channel1.receivedMessages).containsExactly("message 1", "message 2", "message 3");
    assertThat(channel2.receivedMessages).containsExactly("message 1", "message 2", "message 3");
  }

  @Test
  void givenTopic_whenPublishAfterClose_thenThrowsIllegalStateException() {
    // Arrange
    TestChannel<String> channel = new TestChannel<>();
    List<Channel<String>> channels = List.of(channel);
    Topic<String> topic = new DefaultTopic<>(channels);

    // Act
    topic.close();

    // Assert
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> topic.publish("too late")).withMessageContaining("closed");
  }

  @Test
  void givenTopic_whenClosed_thenAllChannelsAreClosed() {
    // Arrange
    TestChannel<String> channel1 = new TestChannel<>();
    TestChannel<String> channel2 = new TestChannel<>();
    List<Channel<String>> channels = List.of(channel1, channel2);
    Topic<String> topic = new DefaultTopic<>(channels);

    // Act
    topic.close();

    // Assert
    assertThat(channel1.closed).isTrue();
    assertThat(channel2.closed).isTrue();
  }

  @Test
  @Timeout(5)
  void givenTopicWithBlockingChannel_whenPublishMessage_thenStallsMetricIncreases()
      throws InterruptedException {
    // Arrange
    Lock lock = new ReentrantLock();
    Condition blocking = lock.newCondition();
    Condition waiting = lock.newCondition();
    BlockingChannel<String> channel = new BlockingChannel<>(lock, blocking, waiting);
    List<Channel<String>> channels = List.of(channel);

    Topic<String> topic = new DefaultTopic<>(channels);
    try {
      Thread publishThread = new Thread(() -> {
        try {
          topic.publish("message"); // This one should stall
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      // Start the thread and wait for it to block
      lock.lock();
      try {
        publishThread.start();
        waiting.await();
        blocking.signal();
      } finally {
        lock.unlock();
      }

      // Wait for thread to complete
      publishThread.join();
    } finally {
      topic.close();
    }

    // Assert
    assertThat(channel.received).isEqualTo(List.of("message"));
    assertThat(topic.checkMetrics().stalls()).isGreaterThan(0);
  }

  @Test
  void givenTopic_whenFlushMetrics_thenMetricsReset() throws InterruptedException {
    // Arrange
    TestChannel<String> channel = new TestChannel<>();
    List<Channel<String>> channels = List.of(channel);

    // Act - publish some messages
    Topic.Metrics initialMetrics, flushedMetrics, resetMetrics;
    try (Topic<String> topic = new DefaultTopic<>(channels)) {
      topic.publish("message 1");
      topic.publish("message 2");

      // Get metrics and check they're correct
      initialMetrics = topic.checkMetrics();

      // Flush metrics
      flushedMetrics = topic.flushMetrics();

      // Check new metrics are reset
      resetMetrics = topic.checkMetrics();
    }

    // Assert
    assertThat(initialMetrics.published()).isEqualTo(2);
    assertThat(flushedMetrics.published()).isEqualTo(2);
    assertThat(resetMetrics.published()).isEqualTo(0);
  }

  @Test
  void givenEmptyChannelsList_whenCreateTopic_thenThrowsIllegalArgumentException() {
    // Arrange
    List<Channel<String>> emptyChannels = List.of();

    // Assert
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new DefaultTopic<>(emptyChannels))
        .withMessageContaining("subscribers must not be empty");
  }

  @Test
  void givenNullChannelsList_whenCreateTopic_thenThrowsNullPointerException() {
    // Assert
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new DefaultTopic<>(null));
  }

  @Test
  @Timeout(5)
  void givenMultipleBlockingChannels_whenPublishMessage_thenAllEventuallyReceiveIt()
      throws InterruptedException {
    // Arrange
    Lock lock = new ReentrantLock();
    Condition blocking = lock.newCondition();
    Condition waiting = lock.newCondition();
    int channelCount = 5;
    List<BlockingChannel<String>> blockingChannels = new ArrayList<>();
    List<Channel<String>> channels = new ArrayList<>();

    for (int i = 0; i < channelCount; i++) {
      BlockingChannel<String> channel = new BlockingChannel<>(lock, blocking, waiting);
      blockingChannels.add(channel);
      channels.add(channel);
    }

    Topic<String> topic = new DefaultTopic<>(channels);
    AtomicInteger completedPublishes = new AtomicInteger(0);
    try {
      // Act - start a thread to publish to all channels
      Thread publishThread = new Thread(() -> {
        try {
          topic.publish("test message");
          completedPublishes.incrementAndGet();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      // Unblock each channel one by one
      lock.lock();
      try {
        publishThread.start();
        for (BlockingChannel<String> channel : blockingChannels) {
          channel.waiting.await();
          channel.blocking.signal();
        }
      } finally {
        lock.unlock();
      }

      // Wait for publish to complete
      publishThread.join();
    } finally {
      topic.close();
    }

    // Assert
    assertThat(completedPublishes.get()).isEqualTo(1);
    assertThat(topic.checkMetrics().published()).isEqualTo(channelCount);
    assertThat(topic.checkMetrics().stalls()).isEqualTo(channelCount);

    for (BlockingChannel<String> channel : blockingChannels) {
      assertThat(channel.received).containsExactly("test message");
    }
  }

  // Helper classes for testing

  private static class TestChannel<T> implements Channel<T> {
    List<T> receivedMessages = new ArrayList<>();
    boolean closed = false;

    @Override
    public boolean tryPublish(T message) {
      receivedMessages.add(message);
      return true;
    }

    @Override
    public void publish(T message) throws InterruptedException {
      receivedMessages.add(message);
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public void bind(Binding<T> binding) {
      throw new UnsupportedOperationException();
    }
  }

  private static class BlockingChannel<T> implements Channel<T> {
    public final List<T> received = new CopyOnWriteArrayList<>();
    private final Lock lock;
    private final Condition blocking;
    private final Condition waiting;
    private boolean closed = false;



    public BlockingChannel(Lock lock, Condition blocking, Condition waiting) {
      this.lock = requireNonNull(lock);
      this.blocking = requireNonNull(blocking);
      this.waiting = requireNonNull(waiting);
    }

    @Override
    public boolean tryPublish(T message) {
      // Always return false to force the use of publish()
      return false;
    }

    @Override
    public void publish(T message) throws InterruptedException {
      lock.lock();
      try {
        if (closed)
          throw new IllegalStateException("closed");
        waiting.signalAll();
        blocking.await();
        if (closed)
          throw new IllegalStateException("closed");
        received.add(message);
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void close() {
      lock.lock();
      try {
        closed = true;
        waiting.signalAll();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void bind(Binding<T> binding) {
      throw new UnsupportedOperationException();
    }
  }
}
