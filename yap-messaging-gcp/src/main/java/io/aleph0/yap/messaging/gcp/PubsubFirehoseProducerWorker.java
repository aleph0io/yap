/*-
 * =================================LICENSE_START==================================
 * yap-messaging-gcp
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
package io.aleph0.yap.messaging.gcp;

import static java.util.Objects.requireNonNull;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse;
import com.google.cloud.pubsub.v1.AckResponse;
import com.google.cloud.pubsub.v1.MessageReceiverWithAckResponse;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.messaging.core.Acknowledgeable;
import io.aleph0.yap.messaging.core.FirehoseMetrics;
import io.aleph0.yap.messaging.core.FirehoseProducerWorker;
import io.aleph0.yap.messaging.core.Message;

/**
 * A {@link FirehoseProducerWorker firehose} that receives messages from Google Pubsub and writes
 * them to the given sink as {@link Message yap messages}. Downstream consumers are responsible for
 * {@link Message#ack() acknowledging} or {@link Message#nack() negatively acknowledging} the
 * messages.
 * 
 * <p>
 * In cases of worker or pipeline failure, any attempts to acknowledge or negatively acknowledge a
 * message from this worker in downstream tasks may fail. As such, users should be careful not to
 * mark messages as acknowledged or negatively acknowledged in downstream tasks. As a result, users
 * should be careful not to mark messages as acknowledged or negatively acknowledged until the
 * listener {@link Acknowledgeable.AcknowledgementListener#onSuccess() confirms} it was acknowledged
 * or negatively acknowledged successfully.
 * 
 * <p>
 * This worker does not fail on acknowledgement or negative acknowledgement failure. Instead, it
 * will redeliver messages at some point in the future according to the configuration of the Pubsub
 * subscription being consumed. If users wish to fail the pipeline on acknowledgement or negative
 * acknowledgement failure, they should do so in the downstream steps responsible for performing the
 * acknowledgement or negative acknowledgement.
 * 
 * <p>
 * Since Pubsub implements {@link Subscriber.Builder#setParallelPullCount(int) parallel pulls} at
 * the subscriber level, users should generally prefer increasing parallel pull count over creating
 * multiple workers in a task to increase throughput.
 * 
 * <p>
 * Also, users should generally configure
 * {@link Subscriber.Builder#setFlowControlSettings(com.google.api.gax.batching.FlowControlSettings)
 * flow control} for the subscriber to ensure that the subscriber does not overwhelm the worker with
 * messages. This acts as a soft backpressure mechanism to ensure that the pipeline can keep up with
 * the rate of messages being sent to it.
 */
public class PubsubFirehoseProducerWorker implements FirehoseProducerWorker<Message> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubsubFirehoseProducerWorker.class);

  /**
   * A factory for creating a {@link Subscriber} that will receive messages from PubSub. Each worker
   * will call this factory to create a new subscriber exactly once. The factory should return the
   * subscriber the worker should use to receive messages. Because subscribers have a lifecycle that
   * this worker manages to ensure message delivery, the should return a new subscriber each time.
   */
  @FunctionalInterface
  public static interface SubscriberFactory {
    /**
     * Creates a new {@link Subscriber} that will receive messages from PubSub. This method should
     * return a new subscriber each time it is called. The worker will manage the lifecycle of the
     * subscriber, so the factory should not return a subscriber that is already started or stopped.
     * 
     * @param receiver the receiver the factory should use to create the subscriber
     * @return the new subscriber
     */
    public Subscriber newSubscriber(MessageReceiverWithAckResponse receiver);
  }

  private static class PubsubFirehoseMessage implements Message {
    private final PubsubMessage message;
    private final AckReplyConsumerWithResponse acker;

    public PubsubFirehoseMessage(PubsubMessage message, AckReplyConsumerWithResponse acker) {
      this.message = requireNonNull(message, "message");
      this.acker = requireNonNull(acker, "acker");
    }

    @Override
    public Map<String, String> attributes() {
      return message.getAttributesMap();
    }

    @Override
    public String body() {
      return message.getData().toStringUtf8();
    }

    @Override
    public void ack(AcknowledgementListener listener) {
      final ApiFuture<AckResponse> future = acker.ack();
      ApiFutures.addCallback(future, new ApiFutureCallback<AckResponse>() {
        @Override
        public void onSuccess(AckResponse result) {
          if (result == AckResponse.SUCCESSFUL)
            listener.onSuccess();
          else
            listener.onFailure(new IOException("ack failed with response " + result));
        }

        @Override
        public void onFailure(Throwable t) {
          listener.onFailure(t);
        }
      }, MoreExecutors.directExecutor());
    }

    @Override
    public void nack(AcknowledgementListener listener) {
      final ApiFuture<AckResponse> future = acker.nack();
      ApiFutures.addCallback(future, new ApiFutureCallback<AckResponse>() {
        @Override
        public void onSuccess(AckResponse result) {
          if (result == AckResponse.SUCCESSFUL)
            listener.onSuccess();
          else
            listener.onFailure(new IOException("nack failed with response " + result));
        }

        @Override
        public void onFailure(Throwable t) {
          listener.onFailure(t);
        }
      }, MoreExecutors.directExecutor());
    }
  }

  private final AtomicLong receivedMetric = new AtomicLong(0);
  private final SubscriberFactory subscriberFactory;

  public PubsubFirehoseProducerWorker(SubscriberFactory subscriberFactory) {
    this.subscriberFactory = requireNonNull(subscriberFactory, "subscriberFactory");
  }

  @Override
  public void produce(Sink<Message> sink) throws IOException, InterruptedException {
    try {
      final BlockingQueue<Throwable> failureCause = new ArrayBlockingQueue<>(1);

      final Subscriber subscriber =
          subscriberFactory.newSubscriber(new MessageReceiverWithAckResponse() {
            @Override
            public void receiveMessage(PubsubMessage message,
                AckReplyConsumerWithResponse consumer) {
              try {
                sink.put(new PubsubFirehoseMessage(message, consumer));
                receivedMetric.incrementAndGet();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.atWarn().setCause(e).log("Subscriber interrupted. Stopping...");
                failureCause.offer(e);
              }
            }
          });

      subscriber.addListener(new Subscriber.Listener() {
        @Override
        public void failed(Subscriber.State from, Throwable failure) {
          LOGGER.atError().setCause(failure).log("Subscriber failed");
          failureCause.offer(failure);
        }
      }, MoreExecutors.directExecutor());

      subscriber.startAsync().awaitRunning();
      try {
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedException();

        LOGGER.atInfo().log("Subscriber connected");

        Throwable fc = failureCause.take();
        if (fc instanceof Error e)
          throw e;
        if (fc instanceof InterruptedException e)
          throw e;
        if (fc instanceof Exception e)
          throw new ExecutionException(e);
        throw new AssertionError("Unexpected error", fc);
      } finally {
        try {
          subscriber.stopAsync().awaitTerminated();
        } catch (IllegalStateException e) {
          LOGGER.atWarn().setCause(e).log("Subscriber was already stopped. Ignoring...");
        }
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedException();
      }
    } catch (InterruptedException e) {
      LOGGER.atError().setCause(e).log("Subscriber interrupted");
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      LOGGER.atError().setCause(cause).log("Subscriber failed");
      if (cause instanceof Error x)
        throw x;
      if (cause instanceof IOException x)
        throw x;
      if (cause instanceof RuntimeException x)
        throw x;
      throw new AssertionError("Unexpected error", e);
    }
  }

  @Override
  public FirehoseMetrics checkMetrics() {
    final long received = receivedMetric.get();
    return new FirehoseMetrics(received);
  }

  @Override
  public FirehoseMetrics flushMetrics() {
    final FirehoseMetrics result = checkMetrics();
    receivedMetric.set(0);
    return result;
  }
}
