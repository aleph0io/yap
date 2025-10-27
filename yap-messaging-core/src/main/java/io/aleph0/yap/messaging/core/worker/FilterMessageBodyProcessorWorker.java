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
import java.util.function.Predicate;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.messaging.core.Acknowledger;
import io.aleph0.yap.messaging.core.Message;

public class FilterMessageBodyProcessorWorker<T>
    extends AcknowledgingMessageBodyProcessorWorker<T, T> {
  private final Predicate<T> predicate;

  public FilterMessageBodyProcessorWorker(Acknowledger<Message<T>> acknowledger,
      Predicate<T> predicate) {
    super(acknowledger);
    this.predicate = requireNonNull(predicate, "predicate");
  }

  @Override
  protected void doProcessMessageBody(T inputBody, Sink<T> sink) throws Exception {
    if (predicate.test(inputBody))
      sink.put(inputBody);
  }
}
