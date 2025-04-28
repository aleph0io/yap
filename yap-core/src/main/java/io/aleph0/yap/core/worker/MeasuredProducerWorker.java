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

import io.aleph0.yap.core.Measureable;
import io.aleph0.yap.core.ProducerWorker;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.util.NoMetrics;

/**
 * A worker that produces outputs via a {@link Sink sink} and produces {@link Measureable metrics}.
 * 
 * @param <OutputT> the type of the output
 * @param <MetricsT> the type of the metrics
 * 
 * @see ProducerWorker
 */
public interface MeasuredProducerWorker<OutputT, MetricsT>
    extends ProducerWorker<OutputT>, Measureable<MetricsT> {
  /**
   * Create a {@link MeasuredProducerWorker measured worker} that produces {@link NoMetrics empty
   * metrics} from an {@link Measureable unmeasured} {@link ProducerWorker worker}.
   * 
   * @param <OutputT> the type of the output
   * @param worker the unmeasured worker to wrap
   * @return the measured worker
   */
  public static <OutputT> MeasuredProducerWorker<OutputT, NoMetrics> withNoMetrics(
      ProducerWorker<OutputT> worker) {
    if (worker == null)
      throw new NullPointerException("worker");
    return new MeasuredProducerWorker<OutputT, NoMetrics>() {
      @Override
      public void produce(Sink<OutputT> sink) throws Exception {
        worker.produce(sink);
      }

      @Override
      public NoMetrics checkMetrics() {
        return NoMetrics.INSTANCE;
      }

      @Override
      public NoMetrics flushMetrics() {
        return NoMetrics.INSTANCE;
      }
    };
  }
}
