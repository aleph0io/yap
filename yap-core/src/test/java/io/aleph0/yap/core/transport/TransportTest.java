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
package io.aleph0.yap.core.transport;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import io.aleph0.yap.core.transport.channel.DefaultChannel;
import io.aleph0.yap.core.transport.queue.DefaultQueue;
import io.aleph0.yap.core.transport.topic.DefaultTopic;

class TransportTest {

  @Test
  @Timeout(5)
  void givenTopicAndQueue_whenPublishingSingleMessage_thenMessageIsReceived()
      throws InterruptedException {
    // Arrange
    Channel<String> channel = new DefaultChannel<>();
    List<Channel<String>> channels = List.of(channel);
    Topic<String> topic = new DefaultTopic<>(channels);
    Queue<String> queue = DefaultQueue.<String>builder().setCapacity(10).build(List.of(channel));

    // Act
    topic.publish("test message");
    String received = queue.receive();

    // Assert
    assertThat(received).isEqualTo("test message");
  }

  @Test
  @Timeout(5)
  void givenTopicAndQueue_whenTopicIsClosed_thenQueueEventuallyDrains()
      throws InterruptedException {
    // Arrange
    Channel<String> channel = new DefaultChannel<>();
    List<Channel<String>> channels = List.of(channel);
    Topic<String> topic = new DefaultTopic<>(channels);
    Queue<String> queue = DefaultQueue.<String>builder().setCapacity(10).build(List.of(channel));

    // Act
    topic.publish("message before close");
    topic.close();
    String received = queue.receive();

    // Assert
    assertThat(received).isEqualTo("message before close");
    assertThat(queue.isDrained()).isTrue();
    assertThat(queue.receive()).isNull();
  }

  @ValueSource(ints = {1, 5, 100})
  @ParameterizedTest
  @Timeout(10)
  void givenTopicAndQueue_whenPublishingMultipleMessages_thenAllMessagesAreReceived(
      int messageCount) throws InterruptedException {
    // Arrange
    Channel<Integer> channel = new DefaultChannel<>();
    List<Channel<Integer>> channels = List.of(channel);
    Topic<Integer> topic = new DefaultTopic<>(channels);
    Queue<Integer> queue =
        DefaultQueue.<Integer>builder().setCapacity(messageCount).build(List.of(channel));

    // Act
    for (int i = 0; i < messageCount; i++) {
      topic.publish(i);
    }
    topic.close();

    List<Integer> received = new ArrayList<>();
    Integer msg;
    while ((msg = queue.receive()) != null) {
      received.add(msg);
    }

    // Assert
    assertThat(received).hasSize(messageCount);
    assertThat(received).containsExactlyElementsOf(
        IntStream.range(0, messageCount).boxed().collect(Collectors.toList()));
    assertThat(queue.isDrained()).isTrue();
  }

  @Test
  @Timeout(10)
  void givenTopicAndMultipleQueues_whenPublishing_thenAllQueuesReceiveMessages()
      throws InterruptedException {
    // Arrange
    Channel<String> channel1 = new DefaultChannel<>();
    Channel<String> channel2 = new DefaultChannel<>();
    List<Channel<String>> channels = List.of(channel1, channel2);
    Topic<String> topic = new DefaultTopic<>(channels);

    Queue<String> queue1 = DefaultQueue.<String>builder().setCapacity(10).build(List.of(channel1));
    Queue<String> queue2 = DefaultQueue.<String>builder().setCapacity(10).build(List.of(channel2));

    // Act
    topic.publish("test message");
    topic.close();

    String received1 = queue1.receive();
    String received2 = queue2.receive();

    // Assert
    assertThat(received1).isEqualTo("test message");
    assertThat(received2).isEqualTo("test message");
    assertThat(queue1.isDrained()).isTrue();
    assertThat(queue2.isDrained()).isTrue();
  }

  @Test
  @Timeout(20)
  void givenFullQueue_whenPublishingConcurrently_thenNoDeadlockOccurs()
      throws InterruptedException {
    // Arrange
    int queueCapacity = 10;
    Channel<Integer> channel = new DefaultChannel<>();
    List<Channel<Integer>> channels = List.of(channel);
    Topic<Integer> topic = new DefaultTopic<>(channels);
    Queue<Integer> queue =
        DefaultQueue.<Integer>builder().setCapacity(queueCapacity).build(List.of(channel));

    // Set up producer thread that will publish more messages than queue capacity
    AtomicBoolean producerDone = new AtomicBoolean(false);
    int totalMessages = queueCapacity * 5;

    Thread producer = new Thread(() -> {
      try {
        for (int i = 0; i < totalMessages; i++) {
          topic.publish(i);
        }
        producerDone.set(true);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    // Set up consumer thread that will consume messages with a slight delay
    AtomicInteger consumed = new AtomicInteger(0);
    Thread consumer = new Thread(() -> {
      try {
        Integer msg;
        while ((msg = queue.receive()) != null) {
          consumed.incrementAndGet();
          // Simulate some processing time
          Thread.sleep(5);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    // Act
    producer.start();
    consumer.start();

    // Wait for producer to complete
    producer.join();
    assertThat(producerDone.get()).isTrue();

    // Close topic to signal consumer to finish
    topic.close();

    // Wait for consumer to process all messages
    consumer.join();

    // Assert
    assertThat(consumed.get()).isEqualTo(totalMessages);
    assertThat(queue.isDrained()).isTrue();
  }

  @Test
  @Timeout(20)
  void givenTopicAndQueue_whenMultipleProducersAndConsumers_thenAllMessagesAreProcessedCorrectly()
      throws Exception {
    // Arrange
    int queueCapacity = 50;
    Channel<Integer> channel = new DefaultChannel<>();
    List<Channel<Integer>> channels = List.of(channel);
    Topic<Integer> topic = new DefaultTopic<>(channels);
    Queue<Integer> queue =
        DefaultQueue.<Integer>builder().setCapacity(queueCapacity).build(List.of(channel));

    int producerCount = 5;
    int messagesPerProducer = 100;
    int totalMessages = producerCount * messagesPerProducer;

    ExecutorService producerExecutor = Executors.newFixedThreadPool(producerCount);
    ExecutorService consumerExecutor = Executors.newFixedThreadPool(3);

    ConcurrentMap<Integer, Integer> receivedCounts = new ConcurrentHashMap<>();
    CountDownLatch producersLatch = new CountDownLatch(producerCount);
    CountDownLatch consumersLatch = new CountDownLatch(totalMessages);

    // Set up producers
    for (int p = 0; p < producerCount; p++) {
      final int producerId = p;
      producerExecutor.submit(() -> {
        try {
          for (int i = 0; i < messagesPerProducer; i++) {
            int message = producerId * messagesPerProducer + i;
            topic.publish(message);
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          producersLatch.countDown();
        }
      });
    }

    // Set up consumers
    for (int c = 0; c < 3; c++) {
      consumerExecutor.submit(() -> {
        try {
          while (!Thread.currentThread().isInterrupted()) {
            Integer msg = queue.receive();
            if (msg == null)
              break;

            receivedCounts.compute(msg, (k, v) -> v == null ? 1 : v + 1);
            consumersLatch.countDown();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }

    // Act
    boolean producersFinished = producersLatch.await(10, TimeUnit.SECONDS);
    topic.close();
    boolean consumersFinished = consumersLatch.await(10, TimeUnit.SECONDS);

    producerExecutor.shutdown();
    consumerExecutor.shutdown();

    // Assert
    assertThat(producersFinished).isTrue();
    assertThat(consumersFinished).isTrue();
    assertThat(receivedCounts).hasSize(totalMessages);
    for (int i = 0; i < totalMessages; i++) {
      assertThat(receivedCounts).containsKey(i);
      assertThat(receivedCounts.get(i)).isEqualTo(1);
    }
    assertThat(queue.isDrained()).isTrue();
  }

  @Test
  @Timeout(10)
  void given_SlowConsumer_when_QueueFills_then_MetricsTrackStalls() throws InterruptedException {
    // Arrange
    int queueCapacity = 5;
    Channel<Integer> channel = new DefaultChannel<>();
    List<Channel<Integer>> channels = List.of(channel);
    Topic<Integer> topic = new DefaultTopic<>(channels);
    Queue<Integer> queue =
        DefaultQueue.<Integer>builder().setCapacity(queueCapacity).build(List.of(channel));

    CountDownLatch publishLatch = new CountDownLatch(queueCapacity + 1); // +1 for the one that will
                                                                         // block
    AtomicBoolean publisherBlocked = new AtomicBoolean(false);

    // Start a producer thread that will send more messages than the queue capacity
    Thread producer = new Thread(() -> {
      try {
        // First fill the queue to capacity
        for (int i = 0; i < queueCapacity; i++) {
          topic.publish(i);
          publishLatch.countDown();
        }

        // This publish should block because the queue is full
        Thread blockDetector = new Thread(() -> {
          try {
            Thread.sleep(100); // Give time for the main thread to enter blocking state
            publisherBlocked.set(true);
            publishLatch.countDown();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
        blockDetector.start();

        // This should block until the consumer takes at least one message
        topic.publish(queueCapacity);

      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    producer.start();

    // Wait until we've confirmed the producer is blocked
    boolean latchComplete = publishLatch.await(5, TimeUnit.SECONDS);
    assertThat(latchComplete).isTrue();
    assertThat(publisherBlocked.get()).isTrue();

    // Now consume messages to unblock the producer
    for (int i = 0; i <= queueCapacity; i++) {
      Integer received = queue.receive();
      assertThat(received).isEqualTo(i);
    }

    // Wait for producer to finish
    producer.join();

    // Assert metrics show stalls occurred
    Queue.Metrics queueMetrics = queue.flushMetrics();
    Topic.Metrics topicMetrics = topic.flushMetrics();

    assertThat(queueMetrics.produced()).isEqualTo(queueCapacity + 1);
    assertThat(queueMetrics.consumed()).isEqualTo(queueCapacity + 1);
    assertThat(queueMetrics.stalls()).isGreaterThan(0);
    assertThat(topicMetrics.stalls()).isGreaterThan(0);
  }
}
