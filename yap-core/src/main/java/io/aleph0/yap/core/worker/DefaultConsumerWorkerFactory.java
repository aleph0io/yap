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
import io.aleph0.yap.core.Source;

/**
 * A {@link ConsumerWorkerFactory} that creates and manages multiple {@link MeasuredConsumerWorker}s
 * and tracks their metrics.
 * 
 * @param <InputT> the type of the input
 * @param <MetricsT> the type of the metrics
 */
public class DefaultConsumerWorkerFactory<InputT, MetricsT>
    implements ConsumerWorkerFactory<InputT, MetricsT> {
  private final List<MeasuredConsumerWorker<InputT, MetricsT>> workers =
      new CopyOnWriteArrayList<>();

  private final MeasuredConsumerWorker<InputT, MetricsT> workerSupplier;
  private final BinaryOperator<MetricsT> metricsAggregator;

  public DefaultConsumerWorkerFactory(MeasuredConsumerWorker<InputT, MetricsT> workerSupplier,
      BinaryOperator<MetricsT> metricsAggregator) {
    this.workerSupplier = requireNonNull(workerSupplier, "workerSupplier");
    this.metricsAggregator = requireNonNull(metricsAggregator, "metricsAggregator");
  }

  @Override
  public MeasuredConsumerWorker<InputT, MetricsT> newConsumerWorker() {
    return new MeasuredConsumerWorker<InputT, MetricsT>() {
      @Override
      public void consume(Source<InputT> source) throws Exception {
        workers.add(this);
        try {
          workerSupplier.consume(source);
        } finally {
          workers.remove(this);
        }
      }

      @Override
      public MetricsT checkMetrics() {
        return workerSupplier.checkMetrics();
      }

      @Override
      public MetricsT flushMetrics() {
        return workerSupplier.flushMetrics();
      }
    };
  }

  @Override
  public MetricsT checkMetrics() {
    MetricsT result = null;
    for (MeasuredConsumerWorker<InputT, MetricsT> worker : workers) {
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
    for (MeasuredConsumerWorker<InputT, MetricsT> worker : workers) {
      final MetricsT metrics = worker.flushMetrics();
      if (result == null)
        result = metrics;
      else
        result = metricsAggregator.apply(result, metrics);
    }
    return result;
  }
}
