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
import io.aleph0.yap.core.Source;

/**
 * A {@link ProcessorWorkerFactory} that creates and manages multiple
 * {@link MeasuredProcessorWorker}s and tracks their metrics.
 * 
 * @param <InputT> the type of the input
 * @param <OutputT> the type of the output
 * @param <MetricsT> the type of the metrics
 */
public class DefaultProcessorWorkerFactory<InputT, OutputT, MetricsT>
    implements ProcessorWorkerFactory<InputT, OutputT, MetricsT> {
  private final List<MeasuredProcessorWorker<InputT, OutputT, MetricsT>> workers =
      new CopyOnWriteArrayList<>();

  private final Supplier<MeasuredProcessorWorker<InputT, OutputT, MetricsT>> workerSupplier;
  private final BinaryOperator<MetricsT> metricsAggregator;

  public DefaultProcessorWorkerFactory(
      Supplier<MeasuredProcessorWorker<InputT, OutputT, MetricsT>> workerSupplier,
      BinaryOperator<MetricsT> metricsAggregator) {
    this.workerSupplier = requireNonNull(workerSupplier, "workerSupplier");
    this.metricsAggregator = requireNonNull(metricsAggregator, "metricsAggregator");
  }

  @Override
  public MeasuredProcessorWorker<InputT, OutputT, MetricsT> newProcessorWorker() {
    final MeasuredProcessorWorker<InputT, OutputT, MetricsT> newWorker = workerSupplier.get();
    return new MeasuredProcessorWorker<InputT, OutputT, MetricsT>() {
      @Override
      public void process(Source<InputT> source, Sink<OutputT> sink) throws Exception {
        workers.add(this);
        try {
          newWorker.process(source, sink);
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
    for (MeasuredProcessorWorker<InputT, OutputT, MetricsT> worker : workers) {
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
    for (MeasuredProcessorWorker<InputT, OutputT, MetricsT> worker : workers) {
      final MetricsT metrics = worker.flushMetrics();
      if (result == null)
        result = metrics;
      else
        result = metricsAggregator.apply(result, metrics);
    }
    return result;
  }
}
