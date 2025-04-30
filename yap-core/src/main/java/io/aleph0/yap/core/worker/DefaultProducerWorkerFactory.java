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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import io.aleph0.yap.core.Sink;

/**
 * A {@link ProducerWorkerFactory} that creates and manages multiple {@link MeasuredProducerWorker}s
 * and tracks their metrics.
 * 
 * <p>
 * This factory uses a {@link BinaryOperator} to aggregate the metrics of all active workers. If
 * there are no active workers, the metrics will be null. If there is one active worker, the metrics
 * will be the metrics of that worker. If there are multiple active workers, the metrics will be the
 * result of applying the {@link BinaryOperator} to the metrics of all active workers.
 * 
 * @param <OutputT> the type of the output
 * @param <MetricsT> the type of the metrics
 */
public class DefaultProducerWorkerFactory<OutputT, MetricsT>
    implements ProducerWorkerFactory<OutputT, MetricsT> {
  private final List<MeasuredProducerWorker<OutputT, MetricsT>> workers =
      new CopyOnWriteArrayList<>();

  private final Supplier<MeasuredProducerWorker<OutputT, MetricsT>> workerSupplier;
  private final BinaryOperator<MetricsT> metricsAggregator;

  public DefaultProducerWorkerFactory(
      Supplier<MeasuredProducerWorker<OutputT, MetricsT>> workerSupplier,
      BinaryOperator<MetricsT> metricsAggregator) {
    this.workerSupplier = requireNonNull(workerSupplier, "workerSupplier");
    this.metricsAggregator = requireNonNull(metricsAggregator, "metricsAggregator");
  }

  @Override
  public MeasuredProducerWorker<OutputT, MetricsT> newProducerWorker() {
    final MeasuredProducerWorker<OutputT, MetricsT> newWorker = workerSupplier.get();
    return new MeasuredProducerWorker<OutputT, MetricsT>() {
      @Override
      public void produce(Sink<OutputT> sink) throws Exception {
        workers.add(this);
        try {
          newWorker.produce(sink);
        } finally {
          workers.remove(this);
        }
      }

      @Override
      public MetricsT checkMetrics() {
        return newWorker.checkMetrics();
      }

      @Override
      public MetricsT flushMetrics() {
        return newWorker.flushMetrics();
      }
    };
  }

  @Override
  public MetricsT checkMetrics() {
    MetricsT result = null;
    for (MeasuredProducerWorker<OutputT, MetricsT> worker : workers) {
      final MetricsT metrics = worker.checkMetrics();
      if (result == null)
        result = metrics;
      else
        result = metricsAggregator.apply(result, metrics);
    }
    return result;
  }

  @Override
  public MetricsT flushMetrics() {
    MetricsT result = null;
    for (MeasuredProducerWorker<OutputT, MetricsT> worker : workers) {
      final MetricsT metrics = worker.flushMetrics();
      if (result == null)
        result = metrics;
      else
        result = metricsAggregator.apply(result, metrics);
    }
    return result;
  }
}
