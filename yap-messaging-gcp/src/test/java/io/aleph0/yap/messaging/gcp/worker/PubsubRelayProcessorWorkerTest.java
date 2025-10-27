/*-
 * =================================LICENSE_START==================================
 * yap-messaging-gcp
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
package io.aleph0.yap.messaging.gcp.worker;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.messaging.core.RelayMetrics;
import io.aleph0.yap.messaging.gcp.worker.PubsubRelayProcessorWorker.MessageExtractor;
import io.aleph0.yap.messaging.gcp.worker.PubsubRelayProcessorWorker.PublisherFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@Testcontainers
public class PubsubRelayProcessorWorkerTest {

  private static final String PROJECT_ID = "test-project";

  @Container
  private final PubSubEmulatorContainer emulator = new PubSubEmulatorContainer(
      DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"));

  private ManagedChannel channel;
  private TransportChannelProvider channelProvider;
  private NoCredentialsProvider credentialsProvider;
  private TopicAdminClient topicAdminClient;
  private SubscriptionAdminClient subscriptionAdminClient;
  private ProjectTopicName topicName;
  private ProjectSubscriptionName subscriptionName;
  private Thread workerThread;

  // Test data
  static class TestMessage {
    private final String id;
    private final String content;
    private final Map<String, String> attributes;

    public TestMessage(String id, String content, Map<String, String> attributes) {
      this.id = id;
      this.content = content;
      this.attributes = attributes;
    }

    public String getId() {
      return id;
    }

    public String getContent() {
      return content;
    }

    public Map<String, String> getAttributes() {
      return attributes;
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    // Set up connection to emulator
    channel =
        ManagedChannelBuilder.forTarget(emulator.getEmulatorEndpoint()).usePlaintext().build();

    channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    credentialsProvider = NoCredentialsProvider.create();

    // Create topic and subscription admin clients
    TopicAdminSettings topicAdminSettings =
        TopicAdminSettings.newBuilder().setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider).build();

    SubscriptionAdminSettings subscriptionAdminSettings =
        SubscriptionAdminSettings.newBuilder().setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider).build();

    topicAdminClient = TopicAdminClient.create(topicAdminSettings);
    subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings);

    // Create a unique topic and subscription
    String uuid = UUID.randomUUID().toString().substring(0, 8);
    topicName = ProjectTopicName.of(PROJECT_ID, "test-topic-" + uuid);
    subscriptionName = ProjectSubscriptionName.of(PROJECT_ID, "test-sub-" + uuid);

    // Create the topic and subscription
    topicAdminClient.createTopic(topicName);
    subscriptionAdminClient.createSubscription(subscriptionName, topicName,
        PushConfig.getDefaultInstance(), 10);
  }

  @AfterEach
  void tearDown() throws Exception {
    // Clean up resources
    if (workerThread != null && workerThread.isAlive()) {
      workerThread.interrupt();
      workerThread.join(5000);
    }

    if (subscriptionAdminClient != null) {
      try {
        subscriptionAdminClient.deleteSubscription(subscriptionName);
      } catch (Exception e) {
        // Ignore
      }
      subscriptionAdminClient.close();
    }

    if (topicAdminClient != null) {
      try {
        topicAdminClient.deleteTopic(topicName);
      } catch (Exception e) {
        // Ignore
      }
      topicAdminClient.close();
    }

    if (channel != null && !channel.isShutdown()) {
      try {
        channel.shutdownNow();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  @Test
  void testPubsubRelayProcessorWorker() throws Exception {
    // Create test messages
    int messageCount = 3;
    List<TestMessage> testMessages = new ArrayList<>();
    for (int i = 0; i < messageCount; i++) {
      Map<String, String> attributes = new HashMap<>();
      attributes.put("key", "value");
      testMessages.add(new TestMessage("msg-" + i, "content-" + i, attributes));
    }

    // Create our source that will provide the test messages
    BlockingQueue<TestMessage> sourceQueue = new LinkedBlockingQueue<>(testMessages);
    Source<TestMessage> source = new Source<TestMessage>() {
      @Override
      public TestMessage tryTake() {
        return sourceQueue.poll();
      }

      @Override
      public TestMessage take() throws InterruptedException {
        return tryTake();
      }

      @Override
      public TestMessage take(Duration timeout) throws InterruptedException, TimeoutException {
        return tryTake();
      }
    };

    // Create a sink to collect the processed messages
    BlockingQueue<TestMessage> sinkQueue = new LinkedBlockingQueue<>();
    Sink<TestMessage> sink = new Sink<TestMessage>() {
      @Override
      public void put(TestMessage message) throws InterruptedException {
        sinkQueue.add(message);
      }
    };

    // Create a collector for messages received from the subscription
    BlockingQueue<PubsubMessage> receivedFromPubsub = new LinkedBlockingQueue<>();
    CountDownLatch receiveLatch = new CountDownLatch(messageCount);

    // Set up a subscriber to verify messages are published
    MessageReceiver receiver = (message, consumer) -> {
      receivedFromPubsub.add(message);
      receiveLatch.countDown();
      consumer.ack();
    };

    Subscriber subscriber = Subscriber.newBuilder(subscriptionName, receiver)
        .setChannelProvider(channelProvider).setCredentialsProvider(credentialsProvider).build();

    subscriber.startAsync().awaitRunning();

    try {
      // Create publisher factory for relay worker
      PublisherFactory publisherFactory = () -> {
        try {
          return Publisher.newBuilder(topicName).setChannelProvider(channelProvider)
              .setCredentialsProvider(credentialsProvider).build();
        } catch (IOException e) {
          throw new RuntimeException("Failed to create publisher", e);
        }
      };

      // Create message extractor
      MessageExtractor<TestMessage> messageExtractor = (value) -> {
        if (value == null)
          return Collections.emptyList();

        PubsubMessage pubsubMessage =
            PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(value.getContent()))
                .putAllAttributes(value.getAttributes()).build();

        return Collections.singletonList(pubsubMessage);
      };

      // Create the worker
      PubsubRelayProcessorWorker<TestMessage> worker =
          new PubsubRelayProcessorWorker<>(publisherFactory, messageExtractor);

      // Run the worker in a separate thread
      workerThread = new Thread(() -> {
        try {
          worker.process(source, sink);
        } catch (InterruptedException e) {
          // Expected when thread is interrupted
        } catch (IOException e) {
          e.printStackTrace();
        }
      });

      workerThread.start();

      // Wait for all messages to be received through Pub/Sub
      assertThat(receiveLatch.await(10, TimeUnit.SECONDS)).isTrue();

      // Verify we received all messages through the sink
      List<TestMessage> receivedBySink = new ArrayList<>();
      for (int i = 0; i < messageCount; i++) {
        TestMessage message = sinkQueue.poll(5, TimeUnit.SECONDS);
        if (message != null) {
          receivedBySink.add(message);
        }
      }

      assertThat(receivedBySink.size()).isEqualTo(messageCount);

      // Verify we received all messages through Pub/Sub
      List<PubsubMessage> pubsubMessages = new ArrayList<>();
      receivedFromPubsub.drainTo(pubsubMessages);

      assertThat(pubsubMessages.size()).isEqualTo(messageCount);

      // Verify message content
      for (int i = 0; i < messageCount; i++) {
        PubsubMessage message = pubsubMessages.get(i);
        String content = message.getData().toStringUtf8();
        assertThat(content).startsWith("content-");
        assertThat(message.getAttributesMap()).containsEntry("key", "value");
      }

      // Check metrics
      RelayMetrics checkedMetrics = worker.checkMetrics();
      assertThat(checkedMetrics.submitted()).isEqualTo(messageCount);
      assertThat(checkedMetrics.acknowledged()).isEqualTo(messageCount);
      assertThat(checkedMetrics.awaiting()).isEqualTo(0);

      // Verify flush metrics resets counters
      RelayMetrics flushedMetrics = worker.flushMetrics();
      assertThat(flushedMetrics.submitted()).isEqualTo(messageCount);
      assertThat(flushedMetrics.acknowledged()).isEqualTo(messageCount);

      assertThat(worker.checkMetrics().submitted()).isEqualTo(0);

      // Wait for worker to complete
      assertThat(workerThread.join(Duration.ofSeconds(5L))).isTrue();
    } finally {
      // Stop the subscriber
      try {
        if (subscriber != null) {
          subscriber.stopAsync().awaitTerminated(5, TimeUnit.SECONDS);
        }
      } catch (Exception e) {
        // Ignore subscriber shutdown errors in test
        System.err.println("Error shutting down subscriber: " + e.getMessage());
      }
    }
  }
}
