package io.aleph0.yap.core.build;

import java.util.List;
import io.aleph0.yap.core.transport.Channel;
import io.aleph0.yap.core.transport.Queue;

public interface QueueBuilder<T> {
  public Queue<T> build(List<Channel<T>> subscriptions);
}
