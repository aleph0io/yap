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
package io.aleph0.yap.messaging.gcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
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
import io.aleph0.yap.messaging.core.Acknowledgeable;
import io.aleph0.yap.messaging.core.FirehoseMetrics;
import io.aleph0.yap.messaging.core.Message;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@Testcontainers
public class PubsubFirehoseProducerWorkerTest {

  private static final String PROJECT_ID = "test-project";

  @Container
  private final PubSubEmulatorContainer emulator = new PubSubEmulatorContainer(
      DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"));

  private TransportChannelProvider channelProvider;
  private CredentialsProvider credentialsProvider;
  private TopicAdminClient topicAdminClient;
  private SubscriptionAdminClient subscriptionAdminClient;
  private ProjectTopicName topicName;
  private ProjectSubscriptionName subscriptionName;
  private Publisher publisher;

  @BeforeEach
  void setupPubsubFirehoseProducerWorkerTest() throws Exception {
    // Set up connection to emulator
    ManagedChannel channel =
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
        PushConfig.getDefaultInstance(), 10); // 10 second ack deadline

    // Create publisher
    publisher = Publisher.newBuilder(topicName).setChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider).build();
  }

  @AfterEach
  void cleanupPubsubFirehoseProducerWorkerTest() throws Exception {
    // Clean up resources
    if (publisher != null) {
      publisher.shutdown();
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
  }

  @Test
  @Timeout(30)
  void testPubsubFirehoseProducerWorker() throws Exception {
    // Create a sink to receive messages
    BlockingQueue<Message<String>> receivedMessages = new LinkedBlockingQueue<>();
    Sink<Message<String>> sink = new Sink<Message<String>>() {
      @Override
      public void put(Message<String> message) throws InterruptedException {
        receivedMessages.offer(message);
      }
    };

    // Create a subscriber factory
    PubsubFirehoseProducerWorker.SubscriberFactory subscriberFactory = receiver -> Subscriber
        .newBuilder(subscriptionName, receiver).setChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider).build();

    // Create the worker
    PubsubFirehoseProducerWorker worker = new PubsubFirehoseProducerWorker(subscriberFactory);

    // Start the worker in a separate thread
    final CountDownLatch workerStartLatch = new CountDownLatch(1);
    Thread workerThread = new Thread(() -> {
      workerStartLatch.countDown();
      try {
        worker.produce(sink);
      } catch (InterruptedException e) {
        // Expected when we interrupt the thread
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    workerThread.start();

    // Give the worker time to start
    workerStartLatch.await();

    // Publish test messages
    int messageCount = 5;
    List<String> messageTexts = new ArrayList<>();
    for (int i = 0; i < messageCount; i++) {
      String messageText = "test-message-" + i;
      messageTexts.add(messageText);
      PubsubMessage message = PubsubMessage.newBuilder()
          .setData(ByteString.copyFromUtf8(messageText)).putAttributes("key", "value").build();
      publisher.publish(message).get();
    }

    // Wait for all messages to be received
    List<Message<String>> messages = new ArrayList<>();
    for (int i = 0; i < messageCount; i++) {
      Message<String> message = receivedMessages.poll(10, TimeUnit.SECONDS);
      if (message == null) {
        fail("Timed out waiting for message " + i);
      }
      messages.add(message);
    }

    // Verify messages
    for (int i = 0; i < messageCount; i++) {
      Message<String> message = messages.get(i);
      assertThat(messageTexts).contains(message.body());
      assertThat(message.attributes()).containsEntry("key", "value");
    }

    // Test acknowledgement on ONE MESSAGE ONLY. Explicitly DO NOT ack all messages. We need to test
    // that the worker doesn't hang forever if we don't ack all messages.
    CountDownLatch ackLatch = new CountDownLatch(1);
    AtomicBoolean ackSuccess = new AtomicBoolean(true);
    messages.get(0).ack(new Acknowledgeable.AcknowledgementListener() {
      @Override
      public void onSuccess() {
        ackLatch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        t.printStackTrace();
        ackSuccess.set(false);
        ackLatch.countDown();
      }
    });

    assertThat(ackLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(ackSuccess.get()).isTrue();

    // Make sure that there were actually unacked messages
    assertThat(messageCount).isGreaterThan(1);

    // Check metrics
    FirehoseMetrics metrics = worker.checkMetrics();
    assertThat(metrics.received()).isEqualTo(messageCount);

    // Flush and check metrics again
    FirehoseMetrics flushedMetrics = worker.flushMetrics();
    assertThat(flushedMetrics.received()).isEqualTo(messageCount);

    assertThat(worker.checkMetrics().received()).isEqualTo(0);

    // Stop the worker
    workerThread.interrupt();
    assertThat(workerThread.join(Duration.ofSeconds(5L))).isTrue();
  }
}
