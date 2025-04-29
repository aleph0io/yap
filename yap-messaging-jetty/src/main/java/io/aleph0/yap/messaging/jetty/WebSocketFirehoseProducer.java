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
package io.aleph0.yap.messaging.jetty;

import static java.util.Objects.requireNonNull;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.messaging.core.FirehoseMetrics;
import io.aleph0.yap.messaging.core.FirehoseProducerWorker;
import io.aleph0.yap.messaging.core.Message;

public class WebSocketFirehoseProducer implements FirehoseProducerWorker<Message> {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketFirehoseProducer.class);

  @SuppressWarnings("serial")
  private static class NormalCloseException extends IOException {
    public NormalCloseException() {
      super("closed");
    }
  }

  public static interface WebsocketConfigurator {
    default void configureHttpClient(HttpClient client) {}

    default void configureWebSocketClient(WebSocketClient client) {}

    default void configureClientUpgradeRequest(ClientUpgradeRequest request) {}

    default void configureSession(Session session) {}
  }

  public static WebsocketConfigurator defaultWebsocketConfigurator() {
    return new WebsocketConfigurator() {};
  }

  public static interface MessageFactory {
    public List<Message> newTextMessages(String text);

    public List<Message> newBinaryMessages(ByteBuffer bytes);
  }

  private final AtomicLong receivedMetric = new AtomicLong(0);

  private final URI uri;
  private final WebsocketConfigurator configurator;
  private final MessageFactory messageFactory;

  public WebSocketFirehoseProducer(URI uri, MessageFactory messageFactory) {
    this(uri, defaultWebsocketConfigurator(), messageFactory);
  }

  public WebSocketFirehoseProducer(URI uri, WebsocketConfigurator configurator,
      MessageFactory messageFactory) {
    this.uri = requireNonNull(uri, "uri");
    this.configurator = requireNonNull(configurator, "configurator");
    this.messageFactory = requireNonNull(messageFactory, "messageFactory");
  }

  public void produce(Sink<Message> sink) throws IOException, InterruptedException {
    try {
      final BlockingQueue<Throwable> failureCauses = new ArrayBlockingQueue<>(1);

      // Create the inner HTTP client. The WebSocketClient would create its own if we didn't provide
      // one, but there's not way to configure it if we do that. So create one here, and let the
      // user configure it.
      final HttpClient http = new HttpClient();
      configurator.configureHttpClient(http);

      // Create the WebSocket client. This is the one that will actually connect to the server.
      // We set some sane defaults for key timeouts for correctness, but the user can override
      // them if they want.
      final WebSocketClient websocket = new WebSocketClient(http);
      websocket.setStopTimeout(5000);
      websocket.setIdleTimeout(Duration.ofSeconds(30));
      configurator.configureWebSocketClient(websocket);

      // Start the WebSocket client. This will start the inner HTTP client as well.
      try {
        websocket.start();
      } catch (Exception e) {
        LOGGER.atError().setCause(e).log("Failed to start WebSocket client");
        throw new ExecutionException("Failed to start WebSocket client", e);
      }

      Throwable failureCause;
      try {
        final ClientUpgradeRequest request = new ClientUpgradeRequest();
        configurator.configureClientUpgradeRequest(request);

        final CountDownLatch latch = new CountDownLatch(1);
        websocket.connect(new Session.Listener() {
          private Session session;

          public void onWebSocketOpen(Session s) {
            LOGGER.atInfo().log("WebSocket connected");
            this.session = s;
            configurator.configureSession(session);
          }

          @Override
          public void onWebSocketText(String text) {
            final List<Message> messages;
            try {
              messages = messageFactory.newTextMessages(text);
            } catch (Exception e) {
              LOGGER.atError().setCause(e).log("Failed to create text messages");
              failureCauses.offer(e);
              session.close(StatusCode.SERVER_ERROR, null, Callback.NOOP);
              return;
            }

            putMessages(messages);
          }

          @Override
          public void onWebSocketBinary(ByteBuffer payload, Callback callback) {
            final List<Message> messages;
            try {
              messages = messageFactory.newBinaryMessages(payload);
            } catch (Exception e) {
              LOGGER.atError().setCause(e).log("Failed to create binary messages");
              failureCauses.offer(e);
              session.close(StatusCode.SERVER_ERROR, null, Callback.NOOP);
              return;
            }

            putMessages(messages);
          }

          private void putMessages(List<Message> messages) {
            try {
              for (Message message : messages) {
                sink.put(message);
                receivedMetric.incrementAndGet();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              LOGGER.atError().setCause(e).log("Failed to produce event");
              failureCauses.offer(e);
              session.close(StatusCode.NORMAL, null, Callback.NOOP);
            }
          }

          @Override
          public void onWebSocketError(Throwable cause) {
            LOGGER.atError().setCause(cause).log("WebSocket error");
            failureCauses.offer(cause);
            latch.countDown();
          }

          @Override
          public void onWebSocketClose(int statusCode, String reason) {
            LOGGER.atInfo().addKeyValue("statusCode", statusCode).addKeyValue("reason", reason)
                .log("WebSocket closed");
            failureCauses.offer(new NormalCloseException());
            latch.countDown();
          }
        }, uri, request);

        latch.await();

        failureCause = failureCauses.take();
      } finally {
        try {
          websocket.stop();
        } catch (Exception e) {
          LOGGER.atError().setCause(e).log("Failed to stop WebSocket client");
          throw new ExecutionException("Failed to stop WebSocket client", e);
        }
      }

      if (failureCause instanceof NormalCloseException) {
        LOGGER.atInfo().log("Websocket session closed normally");
      } else {
        if (failureCause instanceof Error x)
          throw x;
        if (failureCause instanceof InterruptedException)
          throw new InterruptedException();
        if (failureCause instanceof Exception x)
          throw new ExecutionException("Websocket failed", x);
        throw new AssertionError("Unexpected error", failureCause);
      }
    } catch (InterruptedException e) {
      LOGGER.atError().setCause(e).log("Jetstream session interrupted");
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      LOGGER.atError().setCause(cause).log("Jetstream session failed");
      if (cause instanceof IOException)
        throw (IOException) cause;
      if (cause instanceof RuntimeException)
        throw (RuntimeException) cause;
      if (cause instanceof Error)
        throw (Error) cause;
      throw new IOException("Jetstream session failed", cause);
    }
  }

  @Override
  public FirehoseMetrics checkMetrics() {
    final long events = receivedMetric.get();
    return new FirehoseMetrics(events);
  }

  @Override
  public FirehoseMetrics flushMetrics() {
    FirehoseMetrics metrics = checkMetrics();
    receivedMetric.set(0);
    return metrics;
  }
}
