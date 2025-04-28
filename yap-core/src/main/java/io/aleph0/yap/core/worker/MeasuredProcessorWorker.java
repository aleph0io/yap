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
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.core.util.NoMetrics;

/**
 * A worker that takes inputs via a {@link Source source} and produces outputs via a {@link Sink
 * sink} and produces {@link Measureable metrics}.
 * 
 * @param <InputT> the type of the input
 * @param <OutputT> the type of the output
 * @param <MetricsT> the type of the metrics
 * 
 * @see ProcessorWorker
 */
public interface MeasuredProcessorWorker<InputT, OutputT, MetricsT>
    extends ProcessorWorker<InputT, OutputT>, Measureable<MetricsT> {
  /**
   * Create a {@link MeasuredProcessorWorker measured worker} that produces {@link NoMetrics empty
   * metrics} from an {@link Measureable unmeasured} {@link ProcessorWorker worker}.
   * 
   * @param <InputT> the type of the input
   * @param <OutputT> the type of the output
   * @param worker the unmeasured worker to wrap
   * @return the measured worker
   */
  public static <InputT, OutputT> MeasuredProcessorWorker<InputT, OutputT, NoMetrics> withNoMetrics(
      ProcessorWorker<InputT, OutputT> worker) {
    if (worker == null)
      throw new NullPointerException("worker");
    return new MeasuredProcessorWorker<InputT, OutputT, NoMetrics>() {
      @Override
      public void process(Source<InputT> source, Sink<OutputT> sink) throws Exception {
        worker.process(source, sink);
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
