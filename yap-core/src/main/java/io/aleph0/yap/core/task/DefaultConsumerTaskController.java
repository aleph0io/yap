package io.aleph0.yap.core.task;

import java.time.Duration;
import io.aleph0.yap.core.transport.Queue;

public class DefaultConsumerTaskController<InputT> extends DefaultTaskController<InputT, Void> {
  public static <InputT> Builder<InputT> builder() {
    return new Builder<>();
  }

  public static class Builder<InputT>
      implements TaskController.ConsumerTaskControllerBuilder<InputT> {
    private int parallelism = 1;

    public Builder<InputT> setParallelism(int parallelism) {
      if (parallelism < 1)
        throw new IllegalArgumentException("parallelism must be at least 1");
      this.parallelism = parallelism;
      return this;
    }

    private Duration heartbeatInterval = Duration.ofSeconds(60);

    public Builder<InputT> setHeartbeatInterval(Duration heartbeatInterval) {
      if (heartbeatInterval == null)
        throw new NullPointerException();
      if (!heartbeatInterval.isPositive())
        throw new IllegalArgumentException("heartbeatInterval must be positive");
      this.heartbeatInterval = heartbeatInterval;
      return this;
    }

    @Override
    public DefaultConsumerTaskController<InputT> build(Queue<InputT> queue) {
      return new DefaultConsumerTaskController<>(parallelism, heartbeatInterval, queue);
    }
  }

  public DefaultConsumerTaskController(int parallelism, Duration heartbeatInterval,
      Queue<InputT> queue) {
    super(parallelism, heartbeatInterval, queue, null);
  }
}
