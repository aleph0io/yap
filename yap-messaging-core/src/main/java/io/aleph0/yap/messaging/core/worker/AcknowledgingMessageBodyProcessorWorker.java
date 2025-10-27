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

import java.util.Map;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.Sink;
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
public abstract class AcknowledgingMessageBodyProcessorWorker<T, U>
    extends AcknowledgingMessageProcessorWorker<T, U> {
  public AcknowledgingMessageBodyProcessorWorker() {
    this(AcknowledgementFailureHandler.defaultAcknowledgementFailureHandler());
  }

  public AcknowledgingMessageBodyProcessorWorker(AcknowledgementFailureHandler failureHandler) {
    this(new DefaultAcknowledger<>(failureHandler));
  }

  public AcknowledgingMessageBodyProcessorWorker(Acknowledger<Message<T>> acknowledger) {
    super(acknowledger);
  }

  @Override
  protected final void doProcessMessage(String inputId, Map<String, String> inputAttributes,
      T inputBody, Sink<MessageDefinition<U>> sink) throws Exception {
    doProcessMessageBody(inputBody, new Sink<U>() {
      private int counter = 0;

      @Override
      public void put(U outputBody) throws InterruptedException {
        final MessageDefinition<U> outputMessage =
            createOutputMessage(inputId, inputAttributes, inputBody, outputBody, counter++);
        sink.put(outputMessage);
      }
    });
  }

  /**
   * Creates a new output message definition based on the input message's ID, attributes, and body.
   * This method can be overridden by subclasses to customize the output message ID, attributes, or
   * body based on the input message. By default, it appends a counter to the input ID to create a
   * unique output ID and uses the same attributes as the input message.
   * 
   * @param inputId the ID of the input message
   * @param inputAttributes the attributes of the input message
   * @param inputBody the body of the input message
   * @param outputBody the body of the output message
   * @param index the zero-based index of the output message being created for this input message.
   *        The first output message will have a count of 0, the second will have a count of 1, and
   *        so on.
   * @return a new {@link MessageDefinition} for the output message, with a unique ID and the same
   *         attributes as the input message.
   */
  protected MessageDefinition<U> createOutputMessage(String inputId,
      Map<String, String> inputAttributes, T inputBody, U outputBody, int index) {
    final String outputId = inputId + "-" + index;
    final Map<String, String> outputAttributes = inputAttributes;
    return new MessageDefinition<>(outputId, outputAttributes, outputBody);
  }

  protected abstract void doProcessMessageBody(T inputBody, Sink<U> sink) throws Exception;
}
