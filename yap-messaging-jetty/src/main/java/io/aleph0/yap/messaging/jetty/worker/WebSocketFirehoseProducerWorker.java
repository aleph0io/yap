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
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketOpen;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.messaging.core.Acknowledgeable;
import io.aleph0.yap.messaging.core.FirehoseMetrics;
import io.aleph0.yap.messaging.core.Message;
import io.aleph0.yap.messaging.core.worker.FirehoseProducerWorker;

/**
 * A {@link FirehoseProducerWorker} that connects to a WebSocket server and sends messages to a
 * {@link Sink}.
 * 
 * <p>
 * The given {@link MessageFactory} is used to create messages from the text and binary frames
 * received from the WebSocket server. The {@link Configurator} is used to configure the
 * WebSocket client and the HTTP client used to connect to the server.
 * 
 * <p>
 * Because the {@code MessageFactory} is responsible for creating messages, it is also responsible
 * for defining the ack and nack semantics. Users should take care to ensure that the implemented
 * semantics match the {@link Acknowledgeable required semantics}, particularly idempotence and
 * mutually exclusive acks and nacks.
 * 
 * <p>
 * On sink failure, the worker will close the socket gracefully and fail.
 * 
 * <p>
 * On socket error, the worker will simply close the socket and fail.
 * 
 * <p>
 * On interrupt, the worker will attempt to close the connection gracefully. If this takes too long,
 * then it will simply close the socket. The worker will then propagate the interrupt.
 */
public class WebSocketFirehoseProducerWorker<T> implements FirehoseProducerWorker<Message<T>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketFirehoseProducerWorker.class);

  @SuppressWarnings("serial")
  private static class NormalCloseException extends IOException {
    public NormalCloseException() {
      super("closed");
    }
  }

  public static interface Configurator {
    default void configureHttpClient(HttpClient client) {}

    default void configureWebSocketClient(WebSocketClient client) {}

    default void configureClientUpgradeRequest(ClientUpgradeRequest request) {}

    default void configureSession(Session session) {}
  }

  public static Configurator defaultConfigurator() {
    return new Configurator() {};
  }

  public static interface MessageFactory<T> {
    public List<Message<T>> newTextMessages(String text);

    public List<Message<T>> newBinaryMessages(ByteBuffer bytes);
  }

  private final AtomicLong receivedMetric = new AtomicLong(0);

  private final URI uri;
  private final Configurator configurator;
  private final MessageFactory<T> messageFactory;

  public WebSocketFirehoseProducerWorker(URI uri, MessageFactory<T> messageFactory) {
    this(uri, defaultConfigurator(), messageFactory);
  }

  public WebSocketFirehoseProducerWorker(URI uri, Configurator configurator,
      MessageFactory<T> messageFactory) {
    this.uri = requireNonNull(uri, "uri");
    this.configurator = requireNonNull(configurator, "configurator");
    this.messageFactory = requireNonNull(messageFactory, "messageFactory");
  }

  public void produce(Sink<Message<T>> sink) throws IOException, InterruptedException {
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

        websocket.connect(new InternalSocketListener(sink, failureCauses, latch), uri, request);

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
      LOGGER.atError().setCause(e).log("Websocket session interrupted. Propagating...");
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      LOGGER.atError().setCause(cause).log("Websocket session failed. Failing task...");
      if (cause instanceof IOException)
        throw (IOException) cause;
      if (cause instanceof RuntimeException)
        throw (RuntimeException) cause;
      if (cause instanceof Error)
        throw (Error) cause;
      throw new IOException("Jetstream session failed", cause);
    }
  }

  /**
   * This class has to be public so Jetty can see it. It is not intended to be used outside of this
   * package.
   * 
   * <p>
   * We let Jetty manage all the complexity of demand, hence the {@code autoDemand = true}.
   */
  @WebSocket(autoDemand = true)
  public class InternalSocketListener {
    private final Sink<Message<T>> sink;
    private final BlockingQueue<Throwable> failureCauses;
    private final CountDownLatch latch;

    public InternalSocketListener(Sink<Message<T>> sink, BlockingQueue<Throwable> failureCauses,
        CountDownLatch latch) {
      this.sink = requireNonNull(sink, "sink");
      this.failureCauses = requireNonNull(failureCauses, "failureCauses");
      this.latch = requireNonNull(latch, "latch");
    }

    @OnWebSocketOpen
    public void onWebSocketOpen(Session session) {
      LOGGER.atInfo().log("WebSocket connected");
      configurator.configureSession(session);
    }

    @OnWebSocketMessage
    public void onWebSocketText(Session session, String text) {
      final List<Message<T>> messages;
      try {
        messages = messageFactory.newTextMessages(text);
      } catch (Exception e) {
        LOGGER.atError().setCause(e).log("Failed to create text messages");
        failureCauses.offer(e);
        session.close(StatusCode.SERVER_ERROR, null, Callback.NOOP);
        return;
      }

      putMessages(session, messages);
    }

    @OnWebSocketMessage
    public void onWebSocketBinary(Session session, ByteBuffer payload, Callback callback) {
      final List<Message<T>> messages;
      try {
        messages = messageFactory.newBinaryMessages(payload);
      } catch (Exception e) {
        LOGGER.atError().setCause(e).log("Failed to create binary messages");
        failureCauses.offer(e);
        session.close(StatusCode.SERVER_ERROR, null, Callback.NOOP);
        return;
      }

      putMessages(session, messages);

      callback.succeed();
    }

    private void putMessages(Session session, List<Message<T>> messages) {
      try {
        for (Message<T> message : messages) {
          sink.put(message);
          receivedMetric.incrementAndGet();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.atInfo().setCause(e).log("Interrupted while putting messages");
        failureCauses.offer(e);
        session.close(StatusCode.NORMAL, null, Callback.NOOP);
      }
    }

    @OnWebSocketError
    public void onWebSocketError(Session session, Throwable cause) {
      LOGGER.atError().setCause(cause).log("WebSocket error");
      failureCauses.offer(cause);
      latch.countDown();
      session.disconnect();
    }

    @OnWebSocketClose
    public void onWebSocketClose(Session session, int statusCode, String reason) {
      // NOTE: We do not need to send a close frame here, since Jetty will do that for us.
      // At this point, the protocol gods have been appeased, and we just need to close the
      // session and clean up.
      LOGGER.atInfo().addKeyValue("statusCode", statusCode).addKeyValue("reason", reason)
          .log("WebSocket closed");

      final Exception failureCause;
      if (statusCode == StatusCode.NORMAL)
        failureCause = new NormalCloseException();
      else
        failureCause = new IOException("WebSocket closed with status code " + statusCode);

      failureCauses.offer(failureCause);

      latch.countDown();
    }
  }

  @Override
  public FirehoseMetrics checkMetrics() {
    final long received = receivedMetric.get();
    return new FirehoseMetrics(received);
  }

  @Override
  public FirehoseMetrics flushMetrics() {
    FirehoseMetrics result = checkMetrics();
    receivedMetric.set(0);
    return result;
  }
}
