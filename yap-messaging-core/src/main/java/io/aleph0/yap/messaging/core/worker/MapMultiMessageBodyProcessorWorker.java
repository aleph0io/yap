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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.messaging.core.Acknowledger;
import io.aleph0.yap.messaging.core.Message;

public class MapMultiMessageBodyProcessorWorker<T, U>
    extends AcknowledgingMessageBodyProcessorWorker<T, U> {
  @SuppressWarnings("serial")
  private static class UncheckedInterruptedException extends RuntimeException {
    public UncheckedInterruptedException() {
      super("interrupted");
    }
  }

  private final BiConsumer<T, Consumer<U>> mapper;

  public MapMultiMessageBodyProcessorWorker(Acknowledger<Message<T>> acknowledger,
      BiConsumer<T, Consumer<U>> mapper) {
    super(acknowledger);
    this.mapper = requireNonNull(mapper, "mapper");
  }

  @Override
  protected void doProcessMessageBody(T inputBody, Sink<U> sink) throws Exception {
    try {
      mapper.accept(inputBody, outputBody -> {
        try {
          sink.put(outputBody);
        } catch (InterruptedException e) {
          throw new UncheckedInterruptedException();
        }
      });
    } catch (UncheckedInterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    }
  }
}
