package io.aleph0.yap.core.transport;

import io.aleph0.yap.core.Measureable;

public interface Topic<T> extends Measureable<Topic.Metrics>, AutoCloseable {
  public static record Metrics(long published, long stalls) {
    public Metrics {
      if (published < 0)
        throw new IllegalArgumentException("published must be at least zero");
      if (stalls < 0)
        throw new IllegalArgumentException("stalls must be at least zero");
    }
  }

  public void publish(T message) throws InterruptedException;

  @Override
  public void close();
}
