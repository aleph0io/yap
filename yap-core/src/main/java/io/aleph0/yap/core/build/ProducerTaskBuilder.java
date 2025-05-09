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
import io.aleph0.yap.core.task.DefaultProducerTaskController;
import io.aleph0.yap.core.task.TaskController.ProducerTaskControllerBuilder;
import io.aleph0.yap.core.transport.topic.DefaultTopic;
import io.aleph0.yap.core.worker.ProducerWorkerFactory;

public final class ProducerTaskBuilder<OutputT, MetricsT> implements TaskBuilder {
  final String id;
  final ProducerWorkerFactory<OutputT, MetricsT> workerFactory;
  final Set<String> subscribers = new LinkedHashSet<>();
  TopicBuilder<OutputT> topic = DefaultTopic.builder();
  ProducerTaskControllerBuilder<OutputT> controller = DefaultProducerTaskController.builder();

  /* default */ ProducerTaskBuilder(String id,
      ProducerWorkerFactory<OutputT, MetricsT> bodyFactory) {
    this.id = requireNonNull(id);
    this.workerFactory = requireNonNull(bodyFactory);
  }

  @Override
  public String getId() {
    return id;
  }

  public ProducerTaskBuilder<OutputT, MetricsT> setTopic(TopicBuilder<OutputT> topic) {
    this.topic = requireNonNull(topic);
    return this;
  }

  public ProducerTaskBuilder<OutputT, MetricsT> setController(
      ProducerTaskControllerBuilder<OutputT> controller) {
    this.controller = requireNonNull(controller);
    return this;
  }

  public <NextT> ProducerTaskBuilder<OutputT, MetricsT> addSubscriber(
      ProcessorTaskBuilder<OutputT, NextT, ?> subscriber) {
    subscribers.add(subscriber.getId());
    return this;
  }

  public ProducerTaskBuilder<OutputT, MetricsT> addSubscriber(
      ConsumerTaskBuilder<OutputT, ?> subscriber) {
    subscribers.add(subscriber.getId());
    return this;
  }
}
