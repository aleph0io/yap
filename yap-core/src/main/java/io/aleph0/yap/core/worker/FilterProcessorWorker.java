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

import java.util.function.Predicate;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;

/**
 * A {@link ProcessorWorker} that filters items from a source to a sink using a provided
 * {@link Predicate predicate}.
 * 
 * @param <T> the type of the input and output
 */
public class FilterProcessorWorker<T> implements ProcessorWorker<T, T> {
  private final Predicate<T> predicate;

  public FilterProcessorWorker(Predicate<T> predicate) {
    this.predicate = java.util.Objects.requireNonNull(predicate, "predicate");
  }

  @Override
  public void process(Source<T> source, Sink<T> sink) throws InterruptedException {
    for (T item = source.take(); item != null; item = source.take()) {
      if (predicate.test(item))
        sink.put(item);
    }
  }
}
