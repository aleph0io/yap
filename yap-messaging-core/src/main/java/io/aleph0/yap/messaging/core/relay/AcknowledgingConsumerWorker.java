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
package io.aleph0.yap.messaging.core.relay;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.ConsumerWorker;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.messaging.core.Acknowledgeable;

/**
 * A {@link ProcessorWorker} that receives inputs from upstream, acknowledges the input, and then
 * discards the input.
 *
 * <p>
 * On source failure, the worker will fail the task. The worker will attempt to wait for all
 * outstanding acks and nacks to finish before failing the task, but this can be interrupted.
 * 
 * <p>
 * On acknowledgement failure, the worker will fail the task. The worker will attempt to wait for
 * all outstanding acks and nacks to finish before failing the task, but this can be interrupted.
 * 
 * <p>
 * On interrupt, the worker will stop receiving messages. It will not wait for any outstanding acks
 * or nacks to finish before stopping.
 *
 * @param <T> the type of input messages
 */
public class AcknowledgingConsumerWorker<T extends Acknowledgeable> implements ConsumerWorker<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AcknowledgingConsumerWorker.class);

  private final Phaser phaser = new Phaser(1);

  @Override
  public void consume(Source<T> source) throws IOException, InterruptedException {
    try {
      try {
        final AtomicReference<Throwable> failureCause = new AtomicReference<>(null);

        for (T input = source.take(); input != null; input = source.take()) {
          // Check if we have a failure
          throwIfPresent(failureCause);

          // Acknowledge the message
          phaser.register();

          input.ack(new Acknowledgeable.AcknowledgementListener() {
            @Override
            public void onSuccess() {
              phaser.arriveAndDeregister();
            }

            @Override
            public void onFailure(Throwable t) {
              phaser.arriveAndDeregister();
              LOGGER.atError().setCause(t).log("Failed to acknowledge message. Stopping...");
              failureCause.compareAndSet(null, t);
            }
          });
        }

        throwIfPresent(failureCause);
      } finally {
        phaser.arriveAndAwaitAdvance();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.atWarn().setCause(e)
          .log("Interrupted while waiting for acknowledgements. Propagating...");
      throw e;
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      LOGGER.atError().setCause(cause).log("Pubsub firehose failed. Failing task...");
      if (cause instanceof Error x)
        throw x;
      if (cause instanceof IOException x)
        throw x;
      if (cause instanceof RuntimeException x)
        throw x;
      if (cause instanceof Exception x)
        throw new IOException("Pubsub firehose failed", x);
      throw new AssertionError("Unexpected error", e);
    }
  }

  private void throwIfPresent(AtomicReference<Throwable> failureCause)
      throws InterruptedException, ExecutionException {
    Throwable fc = failureCause.get();
    if (fc != null) {
      if (fc instanceof Error x)
        throw x;
      if (fc instanceof InterruptedException)
        throw new InterruptedException();
      if (fc instanceof Exception e)
        throw new ExecutionException(e);
      throw new AssertionError("Unexpected error", fc);
    }
  }
}
