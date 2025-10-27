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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.messaging.core.Acknowledgeable;
import io.aleph0.yap.messaging.core.AcknowledgementFailureHandler;
import io.aleph0.yap.messaging.core.Acknowledger;
import io.aleph0.yap.messaging.core.Message;
import io.aleph0.yap.messaging.core.acknowledger.DefaultAcknowledger;

/**
 * A {@link ProcessorWorker} that receives inputs from upstream, acknowledges the input, and then
 * passes the input downstream. This can be useful when there are optional or non-deterministic
 * downstream processing steps, and you want to ensure that messages are acknowledged as soon as
 * possible, rather than waiting for downstream processing to complete.
 *
 * <p>
 * On source failure, the worker will fail the task.
 * 
 * <p>
 * On sink failure, the worker will fail the task.
 * 
 * <p>
 * On acknowledgement failure, the worker may fail the task, at the discretion of its
 * {@link AcknowledgementFailureHandler failure handler}.
 * 
 * <p>
 * On interrupt, the worker will stop receiving messages immediately.
 * 
 * <p>
 * In all cases, the worker will wait for all outstanding acks to finish before stopping or failing
 * the task.
 *
 * @param <T> the type of input messages
 */
public abstract class AcknowledgingMessageProcessorWorker<T, U>
    extends AcknowledgingProcessorWorker<Message<T>, Message<U>> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AcknowledgingMessageProcessorWorker.class);

  protected static class MessageDefinition<U> {
    private final String id;
    private final Map<String, String> attributes;
    private final U body;

    public MessageDefinition(String id, Map<String, String> attributes, U body) {
      this.id = requireNonNull(id, "id");
      this.attributes = requireNonNull(attributes, "attributes");
      this.body = requireNonNull(body, "body");
    }

    public String id() {
      return id;
    }

    public Map<String, String> attributes() {
      return attributes;
    }

    public U body() {
      return body;
    }

    @Override
    public int hashCode() {
      return Objects.hash(attributes, body, id);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      MessageDefinition other = (MessageDefinition) obj;
      return Objects.equals(attributes, other.attributes) && Objects.equals(body, other.body)
          && Objects.equals(id, other.id);
    }

    @Override
    public String toString() {
      return "MessageDefinition [id=" + id + ", attributes=" + attributes + ", body=" + body + "]";
    }
  }

  public AcknowledgingMessageProcessorWorker() {
    this(AcknowledgementFailureHandler.defaultAcknowledgementFailureHandler());
  }

  public AcknowledgingMessageProcessorWorker(AcknowledgementFailureHandler failureHandler) {
    this(new DefaultAcknowledger<>(failureHandler));
  }

  public AcknowledgingMessageProcessorWorker(Acknowledger<Message<T>> acknowledger) {
    super(acknowledger);
  }

  @Override
  protected final void doProcess(Message<T> input, Sink<Message<U>> sink, Acknowledgeable parent)
      throws Exception {
    final String inputId = input.id();
    final Map<String, String> inputAttributes = input.attributes();
    final T inputBody = input.body();
    doProcessMessage(inputId, inputAttributes, inputBody, output -> {
      sink.put(new Message<U>() {
        private final AtomicBoolean acknowledged = new AtomicBoolean(false);

        @Override
        public String id() {
          return output.id();
        }

        @Override
        public Map<String, String> attributes() {
          return output.attributes();
        }

        @Override
        public U body() {
          return output.body();
        }

        @Override
        public void ack(AcknowledgementListener listener) {
          final boolean updated = acknowledged.compareAndSet(false, true);
          if (updated == true) {
            parent.ack(listener);
          } else {
            LOGGER.atWarn().addKeyValue("messageId", output.id()).addArgument(output.id())
                .log("Attempted to ack message that has already been acknowledged: {}");
          }
        }

        @Override
        public void nack(AcknowledgementListener listener) {
          final boolean updated = acknowledged.compareAndSet(false, true);
          if (updated == true) {
            parent.nack(listener);
          } else {
            LOGGER.atWarn().addKeyValue("messageId", output.id()).addArgument(output.id())
                .log("Attempted to nack message that has already been acknowledged: {}");
          }
        }
      });
    });
  }

  protected abstract void doProcessMessage(String inputId, Map<String, String> inputAttributes,
      T inputBody, Sink<MessageDefinition<U>> sink) throws Exception;
}
