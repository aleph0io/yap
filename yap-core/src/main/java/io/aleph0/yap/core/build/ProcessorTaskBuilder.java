package io.aleph0.yap.core.build;

import static java.util.Objects.requireNonNull;
import java.util.LinkedHashSet;
import java.util.Set;
import io.aleph0.yap.core.task.DefaultProcessorTaskController;
import io.aleph0.yap.core.task.TaskController.ProcessorTaskControllerBuilder;
import io.aleph0.yap.core.transport.queue.DefaultQueue;
import io.aleph0.yap.core.transport.topic.DefaultTopic;
import io.aleph0.yap.core.worker.ProcessorWorkerFactory;

public final class ProcessorTaskBuilder<InputT, OutputT, MetricsT> implements TaskBuilder {
  final String id;
  final ProcessorWorkerFactory<InputT, OutputT, MetricsT> workerFactory;
  final Set<String> subscribers = new LinkedHashSet<>();
  TopicBuilder<OutputT> topic = DefaultTopic.builder();
  QueueBuilder<InputT> queue = DefaultQueue.builder();
  ProcessorTaskControllerBuilder<InputT, OutputT> controller =
      DefaultProcessorTaskController.builder();

  /* default */ ProcessorTaskBuilder(String id,
      ProcessorWorkerFactory<InputT, OutputT, MetricsT> bodyFactory) {
    this.id = requireNonNull(id);
    this.workerFactory = requireNonNull(bodyFactory);
  }

  @Override
  public String getId() {
    return id;
  }

  public ProcessorTaskBuilder<InputT, OutputT, MetricsT> setTopic(TopicBuilder<OutputT> topic) {
    this.topic = requireNonNull(topic);
    return this;
  }

  public ProcessorTaskBuilder<InputT, OutputT, MetricsT> setQueue(QueueBuilder<InputT> queue) {
    this.queue = requireNonNull(queue);
    return this;
  }

  public <NextT> ProcessorTaskBuilder<InputT, OutputT, MetricsT> addSubscriber(
      ProcessorTaskBuilder<OutputT, NextT, MetricsT> subscriber) {
    subscribers.add(subscriber.getId());
    return this;
  }

  public ProcessorTaskBuilder<InputT, OutputT, MetricsT> addSubscriber(
      ConsumerTaskBuilder<OutputT, MetricsT> subscriber) {
    subscribers.add(subscriber.getId());
    return this;
  }
}
