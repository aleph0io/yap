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

import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import io.aleph0.yap.core.build.TopicBuilder;
import io.aleph0.yap.core.transport.Channel;
import io.aleph0.yap.core.transport.Topic;

public class DefaultTopic<T> implements Topic<T> {
  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> implements TopicBuilder<T> {
    @Override
    public Topic<T> build(List<Channel<T>> channels) {
      return new DefaultTopic<>(channels);
    }
  }

  private final AtomicLong sent = new AtomicLong(0);
  private final AtomicLong stalls = new AtomicLong(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final List<Channel<T>> subscribers;

  public DefaultTopic(List<Channel<T>> subscribers) {
    if (subscribers == null)
      throw new NullPointerException();
    if (subscribers.isEmpty())
      throw new IllegalArgumentException("subscribers must not be empty");
    this.subscribers = unmodifiableList(subscribers);
  }

  public void publish(T message) throws InterruptedException {
    if (closed.get())
      throw new IllegalStateException("closed");
    for (Channel<T> subscriber : subscribers) {
      boolean published = subscriber.tryPublish(message);
      if (published == false) {
        stalls.incrementAndGet();
        subscriber.publish(message);
      }
      sent.incrementAndGet();
    }
  }

  @Override
  public void close() {
    if (closed.getAndSet(true) == false) {
      for (Channel<T> subscriber : subscribers)
        subscriber.close();
    }
  }

  @Override
  public Metrics checkMetrics() {
    return new Metrics(sent.get(), stalls.get());
  }

  @Override
  public Metrics flushMetrics() {
    final Metrics result = checkMetrics();
    sent.set(0L);
    stalls.set(0L);
    return result;
  }
}
