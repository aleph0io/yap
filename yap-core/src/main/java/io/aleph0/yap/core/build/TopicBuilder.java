package io.aleph0.yap.core.build;

import java.util.List;
import io.aleph0.yap.core.transport.Channel;
import io.aleph0.yap.core.transport.Topic;

public interface TopicBuilder<T> {
  public Topic<T> build(List<Channel<T>> subscribers);
}
