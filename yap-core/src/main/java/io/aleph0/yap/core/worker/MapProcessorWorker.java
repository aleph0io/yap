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
import java.util.function.Function;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;

/**
 * A {@link ProcessorWorker} that performs a simple one-to-one mapping of items from a source to a
 * sink using a provided function.
 * 
 * @param <X> the type of the input
 * @param <Y> the type of the output
 */
public class MapProcessorWorker<X, Y> implements ProcessorWorker<X, Y> {
  private final Function<X, Y> function;

  public MapProcessorWorker(Function<X, Y> function) {
    this.function = requireNonNull(function, "function");
  }

  @Override
  public void process(Source<X> source, Sink<Y> sink) throws InterruptedException {
    for (X originalItem = source.take(); originalItem != null; originalItem = source.take()) {
      Y mappedItem = function.apply(originalItem);
      sink.put(mappedItem);
    }
  }
}
