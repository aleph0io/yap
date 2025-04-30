package io.aleph0.yap.messaging.core.relay;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.messaging.core.Acknowledgeable;

/**
 * A {@link ProcessorWorker} that receives inputs from upstream, acknowledges the input, and then
 * publishes them to downstream.
 *
 * <p>
 * On sink or source failure, the worker will fail the task. The worker will attempt to wait for all
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
 * @param <T> the type of input and output messages
 */
public class AcknowledgingProcessorWorker<T extends Acknowledgeable>
    implements ProcessorWorker<T, T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AcknowledgingProcessorWorker.class);

  private final Phaser phaser = new Phaser(1);

  @Override
  public void process(Source<T> source, Sink<T> sink) throws IOException, InterruptedException {
    try {
      try {
        final AtomicReference<Throwable> failureCause = new AtomicReference<>(null);

        for (T input = source.take(); input != null; input = source.take()) {
          // Check if we have a failure
          throwIfPresent(failureCause);

          // Acknowledge the message
          phaser.register();

          final T theinput = input;
          input.ack(new Acknowledgeable.AcknowledgementListener() {
            @Override
            public void onSuccess() {
              phaser.arriveAndDeregister();
              try {
                sink.put(theinput);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.atWarn().setCause(e)
                    .log("Interrupted while trying to put message. Stopping...");
                failureCause.compareAndSet(null, e);
              } catch (Throwable t) {
                LOGGER.atError().setCause(t).log("Failed to put message. Stopping...");
                failureCause.compareAndSet(null, t);
              }
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
