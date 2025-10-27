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
package io.aleph0.yap.messaging.core.worker;

import static java.util.Objects.requireNonNull;
import java.util.function.Function;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.messaging.core.acknowledger.NopAcknowledger;

public class MapMessageBodyProcessorWorker<T, U>
    extends AcknowledgingMessageBodyProcessorWorker<T, U> {
  private final Function<T, U> mapper;

  public MapMessageBodyProcessorWorker(Function<T, U> mapper) {
    // A map processor is one-to-one, by definition, so there are no situations where we would want
    // to acknowledge a message because we are dropping it.
    super(new NopAcknowledger<>());
    this.mapper = requireNonNull(mapper, "mapper");
  }

  @Override
  protected void doProcessMessageBody(T inputBody, Sink<U> sink) throws Exception {
    final U outputBody = mapper.apply(inputBody);
    sink.put(outputBody);
  }
}
