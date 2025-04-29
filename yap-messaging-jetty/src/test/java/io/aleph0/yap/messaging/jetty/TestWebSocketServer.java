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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.websocket.server.JettyWebSocketCreator;
import org.eclipse.jetty.ee10.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketOpen;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple WebSocket server that sends text messages to connected clients.
 * 
 * <p>
 * The server is configured to send a limited number of messages, and it can be started and stopped
 * as needed.
 */
public class TestWebSocketServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestWebSocketServer.class);

  public static final int DEFAULT_PATIENCE = 1000;

  public static final int DEFAULT_LIMIT = Integer.MAX_VALUE;

  private final Server server;
  private final int limit;
  private final Supplier<String> eventSupplier;

  public TestWebSocketServer(int port, List<String> events) {
    this(port, events.size(), new Supplier<>() {
      private final Iterator<String> iterator = events.iterator();

      @Override
      public String get() {
        if (!iterator.hasNext())
          throw new IllegalStateException("No more events to send");
        return iterator.next();
      }
    });
  }

  public TestWebSocketServer(int port, int limit, Supplier<String> eventSupplier) {
    if (limit <= 0)
      throw new IllegalArgumentException("limit must be at least 1");
    this.server = new Server(port);
    this.limit = limit;
    this.eventSupplier = requireNonNull(eventSupplier);
  }

  public String endpoint() {
    // Raw server URI example: http://localhost:8080/
    final StringBuilder result = new StringBuilder(server.getURI().toString());
    result.replace(0, 4, "ws");
    result.append("subscribe");
    return result.toString();
  }

  public void start() throws Exception {
    ServletContextHandler context = new ServletContextHandler();
    server.setHandler(context);
    server.setStopTimeout(5000L);

    JettyWebSocketServletContainerInitializer.configure(context, (servletContext, wsContainer) -> {
      wsContainer.addMapping("/subscribe",
          (JettyWebSocketCreator) (req, resp) -> new TestWebSocketEndpoint());
    });

    server.start();
  }

  public void stop() throws Exception {
    server.stop();
  }

  @WebSocket
  public class TestWebSocketEndpoint {
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private final AtomicInteger pending = new AtomicInteger(0);
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private Future<?> publisher;

    @OnWebSocketOpen
    public void onConnect(Session session) {
      publisher = executor.scheduleAtFixedRate(() -> {
        sendMessage(session);
      }, 0, 10L, TimeUnit.MILLISECONDS);
    }

    private void sendMessage(Session session) {
      if (closed.get() == true)
        return;

      int total = count.incrementAndGet();
      if (total > limit) {
        LOGGER.atInfo().addKeyValue("count", total).log("Closing client due to limit of messages");
        closed.set(true);
        publisher.cancel(false);
        session.close(StatusCode.NORMAL, "Goodbye", new Callback() {
          @Override
          public void succeed() {
            LOGGER.atInfo().log("Successfully sent close frame to client");
          }

          @Override
          public void fail(Throwable x) {
            LOGGER.atError().setCause(x).log("Failed to send close frame to client");
          }
        });
        return;
      }

      // We want our messages to be about 550 bytes long, since that's the average size of a
      // "real" commit message.

      final String event = eventSupplier.get();

      session.sendText(event, new Callback() {
        @Override
        public void succeed() {
          if (closed.get() == false) {
            LOGGER.atTrace().log("Sent message to client");
            pending.decrementAndGet();
          }
        }

        @Override
        public void fail(Throwable x) {
          LOGGER.atError().log("Failed to send message to client");
          if (closed.get() == false)
            pending.decrementAndGet();
          // The whole server will get closed later
        }
      });
    }

    @OnWebSocketMessage
    public void onMessage(Session session, String message) {
      LOGGER.atInfo().addKeyValue("message", message)
          .log("Received unexpected message from client");
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
      closed.set(true);
      if (publisher != null)
        publisher.cancel(false);
      executor.shutdown();
    }

    @OnWebSocketError
    public void onError(Session session, Throwable cause) {
      LOGGER.atError().setCause(cause).log("WebSocket error");
      closed.set(true);
      if (publisher != null)
        publisher.cancel(false);
      executor.shutdown();
    }
  }
}
