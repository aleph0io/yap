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

  public ProcessorTaskBuilder<InputT, OutputT, MetricsT> setController(
      ProcessorTaskControllerBuilder<InputT, OutputT> controller) {
    this.controller = requireNonNull(controller);
    return this;
  }

  public <NextT> ProcessorTaskBuilder<InputT, OutputT, MetricsT> addSubscriber(
      ProcessorTaskBuilder<? super OutputT, NextT, ?> subscriber) {
    subscribers.add(subscriber.getId());
    return this;
  }

  public ProcessorTaskBuilder<InputT, OutputT, MetricsT> addSubscriber(
      ConsumerTaskBuilder<? super OutputT, ?> subscriber) {
    subscribers.add(subscriber.getId());
    return this;
  }
}
