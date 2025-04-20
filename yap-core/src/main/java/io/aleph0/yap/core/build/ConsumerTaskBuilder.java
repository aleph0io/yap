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
