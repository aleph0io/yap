/*-
 * =================================LICENSE_START==================================
 * yap-messaging-core
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
package io.aleph0.yap.messaging.core;

import java.util.Collection;
import java.util.function.Function;
import io.aleph0.yap.messaging.core.SimpleSplitGateProcessorWorker.SimpleSplitGateBody;

public class SimpleSplitGateProcessorWorker<X, Y>
    extends SplitGateProcessorWorker<X, SimpleSplitGateBody<X, Y>> {
  public static record SimpleSplitGateBody<X, Y>(X input, Y split) {
  }

  public SimpleSplitGateProcessorWorker(Function<X, Collection<Y>> splitterFunction) {
    super(x -> new SimpleSplitGateBody<>(x, null), y -> splitterFunction.apply(y).stream()
        .map(yi -> new SimpleSplitGateBody<X, Y>(null, yi)).toList());
  }
}
