package io.aleph0.yap.core.task;

import java.time.Duration;
import io.aleph0.yap.core.transport.Topic;

public class DefaultProducerTaskController<OutputT> extends DefaultTaskController<Void, OutputT> {
  public static <OutputT> Builder<OutputT> builder() {
    return new Builder<>();
  }

  public static class Builder<OutputT>
      implements TaskController.ProducerTaskControllerBuilder<OutputT> {
    private int parallelism = 1;

    public Builder<OutputT> setParallelism(int parallelism) {
      if (parallelism < 1)
        throw new IllegalArgumentException("parallelism must be at least 1");
      this.parallelism = parallelism;
      return this;
    }

    private Duration heartbeatInterval = Duration.ofSeconds(60);

    public Builder<OutputT> setHeartbeatInterval(Duration heartbeatInterval) {
      if (heartbeatInterval == null)
        throw new NullPointerException();
      if (!heartbeatInterval.isPositive())
        throw new IllegalArgumentException("heartbeatInterval must be positive");
      this.heartbeatInterval = heartbeatInterval;
      return this;
    }

    @Override
    public DefaultProducerTaskController<OutputT> build(Topic<OutputT> topic) {
      return new DefaultProducerTaskController<>(parallelism, heartbeatInterval, topic);
    }
  }

  public DefaultProducerTaskController(int parallelism, Duration heartbeatInterval,
      Topic<OutputT> topic) {
    super(parallelism, heartbeatInterval, null, topic);
  }
}
