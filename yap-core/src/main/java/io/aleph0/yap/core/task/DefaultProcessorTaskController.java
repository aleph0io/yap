package io.aleph0.yap.core.task;

import java.time.Duration;
import io.aleph0.yap.core.transport.Queue;
import io.aleph0.yap.core.transport.Topic;

public final class DefaultProcessorTaskController<InputT, OutputT>
    extends DefaultTaskController<InputT, OutputT> {
  public static <InputT, OutputT> Builder<InputT, OutputT> builder() {
    return new Builder<>();
  }

  public static class Builder<InputT, OutputT>
      implements TaskController.ProcessorTaskControllerBuilder<InputT, OutputT> {
    private int parallelism = 1;

    public Builder<InputT, OutputT> setParallelism(int parallelism) {
      if (parallelism < 1)
        throw new IllegalArgumentException("parallelism must be at least 1");
      this.parallelism = parallelism;
      return this;
    }

    private Duration heartbeatInterval = Duration.ofSeconds(60);

    public Builder<InputT, OutputT> setHeartbeatInterval(Duration heartbeatInterval) {
      if (heartbeatInterval == null)
        throw new NullPointerException();
      if (!heartbeatInterval.isPositive())
        throw new IllegalArgumentException("heartbeatInterval must be positive");
      this.heartbeatInterval = heartbeatInterval;
      return this;
    }

    @Override
    public TaskController build(Queue<InputT> queue, Topic<OutputT> topic) {
      return new DefaultTaskController<>(parallelism, heartbeatInterval, queue, topic);
    }
  }

  public DefaultProcessorTaskController(int parallelism, Duration heartbeatInterval,
      Queue<InputT> queue, Topic<OutputT> topic) {
    super(parallelism, heartbeatInterval, queue, topic);
  }
}
