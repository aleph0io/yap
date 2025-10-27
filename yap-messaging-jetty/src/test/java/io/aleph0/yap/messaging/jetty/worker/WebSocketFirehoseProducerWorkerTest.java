/*-
 * =================================LICENSE_START==================================
 * yap-messaging-jetty
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
package io.aleph0.yap.messaging.jetty.worker;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.messaging.core.FirehoseMetrics;
import io.aleph0.yap.messaging.core.Message;
import io.aleph0.yap.messaging.jetty.TestWebSocketServer;

public class WebSocketFirehoseProducerWorkerTest {
  public static final int NUM_EVENTS = 10;

  private TestWebSocketServer server;
  private URI serverUri;

  @BeforeEach
  public void setup() throws Exception {
    List<String> events = new ArrayList<>();
    for (int i = 1; i <= NUM_EVENTS; i++)
      events.add("test message");
    // use a random port
    server = new TestWebSocketServer(0, events);
    server.start();
    serverUri = new URI(server.endpoint());
  }

  @AfterEach
  public void teardown() throws Exception {
    server.stop();
  }

  @Test
  public void testProduceTextMessages()
      throws IOException, InterruptedException, URISyntaxException {
    // Given
    final BlockingQueue<Message<String>> sinkQueue = new LinkedBlockingQueue<>();
    final Sink<Message<String>> sink = sinkQueue::offer;

    final WebSocketFirehoseProducerWorker.MessageFactory<String> messageFactory =
        new io.aleph0.yap.messaging.jetty.worker.WebSocketFirehoseProducerWorker.MessageFactory<>() {
          @Override
          public List<Message<String>> newTextMessages(String text) {
            return List.of(new Message<>() {
              private final String id = UUID.randomUUID().toString();

              public String id() {
                return id;
              }

              @Override
              public void ack(AcknowledgementListener listener) {}

              @Override
              public void nack(AcknowledgementListener listener) {}

              @Override
              public Map<String, String> attributes() {
                return Map.of();
              }

              @Override
              public String body() {
                return text;
              }
            });
          }

          @Override
          public List<Message<String>> newBinaryMessages(ByteBuffer bytes) {
            throw new UnsupportedOperationException();
          }
        };

    final WebSocketFirehoseProducerWorker<String> producer =
        new WebSocketFirehoseProducerWorker<>(serverUri, messageFactory);

    producer.produce(sink);

    assertThat(sinkQueue).hasSize(NUM_EVENTS);

    final FirehoseMetrics metrics = producer.checkMetrics();

    assertThat(metrics.received()).isEqualTo(NUM_EVENTS);
  }
}

