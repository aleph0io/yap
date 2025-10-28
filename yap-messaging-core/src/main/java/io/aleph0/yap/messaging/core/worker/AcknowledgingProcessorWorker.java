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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.messaging.core.Acknowledgeable;
import io.aleph0.yap.messaging.core.AcknowledgementFailureHandler;
import io.aleph0.yap.messaging.core.Acknowledger;
import io.aleph0.yap.messaging.core.AcknowledgerMetrics;
import io.aleph0.yap.messaging.core.acknowledger.DefaultAcknowledger;
import io.aleph0.yap.messaging.core.acknowledger.NopAcknowledger;

/**
 * An abstract {@link ProcessorWorker} that receives inputs from upstream, processes the input into
 * zero or more outputs, and acknowledges the input (a) immediately, if zero outputs are created, or
 * (b) after each output has completed processing, if one or more outputs are created. This is
 * intended as a base class for processors that do not necessarily process messages in a strictly
 * one-to-one fashion, but still want to ensure that messages are acknowledged consistently and
 * reliably.
 *
 * <p>
 * On source failure, the worker will fail the task.
 * 
 * <p>
 * On sink failure, the worker will fail the task.
 * 
 * <p>
 * The acknowledgment of dropped messages is handled by the given {@link Acknowledger}. By default,
 * this will be a {@link DefaultAcknowledger} if none is given. Different implementations of the
 * {@link Acknowledger} interface may be used to achieve different effects, e.g., using
 * {@link NopAcknowledger} to disable acknowledgment of dropped messages.
 * 
 * <p>
 * On acknowledgement failure of dropped messages, the worker may fail the task, at the discretion
 * of its {@link AcknowledgementFailureHandler failure handler}.
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
public abstract class AcknowledgingProcessorWorker<T extends Acknowledgeable, U>
    implements ProcessorWorker<T, U> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AcknowledgingProcessorWorker.class);

  private final Acknowledger<T> acknowledger;

  public AcknowledgingProcessorWorker() {
    this(AcknowledgementFailureHandler.defaultAcknowledgementFailureHandler());
  }

  public AcknowledgingProcessorWorker(AcknowledgementFailureHandler acknowledgementFailureHandler) {
    this(new DefaultAcknowledger<>(acknowledgementFailureHandler));
  }

  public AcknowledgingProcessorWorker(Acknowledger<T> acknowledger) {
    this.acknowledger = requireNonNull(acknowledger, "acknowledger");
  }

  @Override
  public void process(Source<T> source, Sink<U> sink) throws Exception {
    try {
      try {
        for (T input = source.take(); input != null; input = source.take()) {
          acknowledger.throwIfPresent();


          final T theinput = input;
          final AtomicInteger counter = new AtomicInteger(0);
          final AtomicInteger retired = new AtomicInteger(0);
          final Acknowledgeable parent = new Acknowledgeable() {
            @Override
            public void ack(AcknowledgementListener listener) {
              final int myretired = retired.incrementAndGet();
              if (myretired == counter.get()) {
                theinput.ack(listener);
              } else if (myretired > counter.get()) {
                LOGGER.atWarn().log("Message retired more times than expected: {} > {}", myretired,
                    counter.get());
              }
            }

            @Override
            public void nack(AcknowledgementListener listener) {
              final int myretired = retired.incrementAndGet();
              if (myretired == counter.get()) {
                theinput.nack(listener);
              } else if (myretired > counter.get()) {
                LOGGER.atWarn().log("Message retired more times than expected: {} > {}", myretired,
                    counter.get());
              }
            }
          };

          // We would much rather pass the events directly to the sink, but we need to know how many
          // outputs we are going to produce before we can acknowledge the input. So we collect them
          // in a list first, and then pass them to the sink.
          final List<U> outputs = new ArrayList<>();
          doProcess(input, outputs::add, parent);
          counter.set(outputs.size());

          // Send any outputs to the sink, or acknowledge the input if no outputs were produced.
          if (counter.get() == 0) {
            acknowledger.acknowledge(input);
          } else {
            for (U output : outputs)
              sink.put(output);
          }
        }

        acknowledger.throwIfPresent();
      } finally {
        acknowledger.close();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.atWarn().setCause(e)
          .log("Interrupted while waiting for acknowledgements. Propagating...");
      throw e;
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      LOGGER.atError().setCause(cause).log("Acknowledger failed. Failing task...");
      if (cause instanceof Error x)
        throw x;
      if (cause instanceof Exception x)
        throw x;
      throw new AssertionError("Unexpected error", e);
    }
  }

  /**
   * Process the input and emit zero or more outputs to the sink. Each output should acknowledge
   * <em>parent</em> when processing is complete, not <em>input</em>. This worker is responsible for
   * acknowledging the input message only after all outputs have acknowledged the given parent.
   * 
   * @param input the input message to process
   * @param sink the sink to emit output messages to
   * @param parent The value to be acknowledged by each input when processing is complete
   * @throws Exception if any error occurs during processing, which will cause the worker to fail.
   */
  protected abstract void doProcess(T input, Sink<U> sink, Acknowledgeable parent) throws Exception;

  protected AcknowledgerMetrics checkAcknowledgerMetrics() {
    return acknowledger.checkMetrics();
  }

  protected AcknowledgerMetrics flushAcknowledgerMetrics() {
    return acknowledger.flushMetrics();
  }
}
