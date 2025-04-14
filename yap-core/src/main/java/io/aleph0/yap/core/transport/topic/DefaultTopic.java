package io.aleph0.yap.core.transport.topic;

import static java.util.Collections.unmodifiableList;
import java.util.List;
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

  private final List<Channel<T>> subscribers;
  private volatile boolean closed;

  public DefaultTopic(List<Channel<T>> subscribers) {
    if (subscribers == null)
      throw new NullPointerException();
    if (subscribers.isEmpty())
      throw new IllegalArgumentException("subscribers must not be empty");
    this.subscribers = unmodifiableList(subscribers);
  }

  public void publish(T message) throws InterruptedException {
    if (closed)
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
    closed = true;
    for (Channel<T> subscriber : subscribers)
      subscriber.close();
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
