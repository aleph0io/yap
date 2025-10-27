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
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.core.worker.MeasuredConsumerWorker;
import io.aleph0.yap.messaging.core.Acknowledgeable;
import io.aleph0.yap.messaging.core.AcknowledgementFailureHandler;
import io.aleph0.yap.messaging.core.Acknowledger;
import io.aleph0.yap.messaging.core.AcknowledgerMetrics;
import io.aleph0.yap.messaging.core.acknowledger.DefaultAcknowledger;

/**
 * A {@link ProcessorWorker} that receives inputs from upstream, acknowledges the input, and then
 * discards the input.
 *
 * <p>
 * On source failure, the worker will fail the task.
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
public class AcknowledgerConsumerWorker<T extends Acknowledgeable>
    implements MeasuredConsumerWorker<T, AcknowledgerMetrics> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AcknowledgerConsumerWorker.class);

  private final Acknowledger<T> acknowledger;

  public AcknowledgerConsumerWorker() {
    this(AcknowledgementFailureHandler.defaultAcknowledgementFailureHandler());
  }

  public AcknowledgerConsumerWorker(AcknowledgementFailureHandler acknowledgementFailureHandler) {
    this(new DefaultAcknowledger<>(acknowledgementFailureHandler));
  }

  public AcknowledgerConsumerWorker(Acknowledger<T> acknowledger) {
    this.acknowledger = requireNonNull(acknowledger, "acknowledger");
  }

  @Override
  public void consume(Source<T> source) throws Exception {
    try {
      try {
        for (T message = source.take(); message != null; message = source.take()) {
          acknowledger.throwIfPresent();
          acknowledger.acknowledge(message);
        }
        acknowledger.throwIfPresent();
      } finally {
        acknowledger.close();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.atWarn().setCause(e).log("Interrupted while waiting for messages. Propagating...");
      throw e;
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      LOGGER.atError().setCause(cause).log("Acknowledgement failed. Failing task...");
      if (cause instanceof Error x)
        throw x;
      if (cause instanceof Exception x)
        throw x;
      throw new AssertionError("Unexpected error", e);
    }
  }

  @Override
  public AcknowledgerMetrics checkMetrics() {
    return acknowledger.checkMetrics();
  }

  @Override
  public AcknowledgerMetrics flushMetrics() {
    return acknowledger.flushMetrics();
  }
}
