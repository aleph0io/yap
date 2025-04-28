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
package io.aleph0.yap.core.worker;

import static java.util.Objects.requireNonNull;

/**
 * A {@link ProducerWorkerFactory} that manages a singleton {@link MeasuredProducerWorker} and
 * tracks its metrics.
 * 
 * <p>
 * This factory will only {@link #newProducerWorker() create} one worker, for which it will return
 * its given worker. All subsequent attempts to create a worker will throw an
 * {@link IllegalStateException}.
 * 
 * @param <OutputT> the type of the worker's output
 * @param <MetricsT> the type of the worker's metrics
 */
public class SingletonProducerWorkerFactory<OutputT, MetricsT>
    implements ProducerWorkerFactory<OutputT, MetricsT> {
  private final MeasuredProducerWorker<OutputT, MetricsT> worker;

  private boolean created = false;

  public SingletonProducerWorkerFactory(MeasuredProducerWorker<OutputT, MetricsT> worker) {
    this.worker = requireNonNull(worker);
  }

  @Override
  public MeasuredProducerWorker<OutputT, MetricsT> newProducerWorker() {
    if (created)
      throw new IllegalStateException("worker already created");
    created = true;
    return worker;
  }

  @Override
  public MetricsT checkMetrics() {
    return worker.checkMetrics();
  }

  @Override
  public MetricsT flushMetrics() {
    return worker.flushMetrics();
  }
}
