package io.aleph0.yap.core.build;

import static java.util.Objects.requireNonNull;
import io.aleph0.yap.core.task.DefaultConsumerTaskController;
import io.aleph0.yap.core.task.TaskController.ConsumerTaskControllerBuilder;
import io.aleph0.yap.core.transport.queue.DefaultQueue;
import io.aleph0.yap.core.worker.ConsumerWorkerFactory;

public final class ConsumerTaskBuilder<InputT, MetricsT> implements TaskBuilder {
  final String id;
  final ConsumerWorkerFactory<InputT, MetricsT> workerFactory;
  QueueBuilder<InputT> queue = DefaultQueue.builder();
  ConsumerTaskControllerBuilder<InputT> controller = DefaultConsumerTaskController.builder();

  /* default */ ConsumerTaskBuilder(String id,
      ConsumerWorkerFactory<InputT, MetricsT> workerFactory) {
    this.id = requireNonNull(id);
    this.workerFactory = requireNonNull(workerFactory);
  }

  @Override
  public String getId() {
    return id;
  }

  public ConsumerTaskBuilder<InputT, MetricsT> setQueue(QueueBuilder<InputT> queue) {
    this.queue = requireNonNull(queue);
    return this;
  }

  public ConsumerTaskBuilder<InputT, MetricsT> setController(
      ConsumerTaskControllerBuilder<InputT> controller) {
    this.controller = requireNonNull(controller);
    return this;
  }
}
