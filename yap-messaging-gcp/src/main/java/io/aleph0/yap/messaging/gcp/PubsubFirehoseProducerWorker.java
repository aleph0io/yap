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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.ApiService.State;
import com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse;
import com.google.cloud.pubsub.v1.AckResponse;
import com.google.cloud.pubsub.v1.MessageReceiverWithAckResponse;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.task.TaskManager;
import io.aleph0.yap.messaging.core.Acknowledgeable;
import io.aleph0.yap.messaging.core.Acknowledgeable.AcknowledgementListener;
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
 * On sink failure, the worker will stop the internal PubSub subscriber and fail the task. All
 * outstanding messages are nacked automatically.
 * 
 * <p>
 * On subscriber failure, the worker will stop the internal PubSub subscriber and fail the task. All
 * outstanding messages are nacked automatically.
 * 
 * <p>
 * On interrupt, the worker will stop the internal PubSub subscriber and propagate the interrupt.
 * All outstanding messages are nacked automatically.
 * 
 * <p>
 * Users should be careful not to mark messages as acknowledged or negatively acknowledged until the
 * listener confirms the operation either {@link Acknowledgeable.AcknowledgementListener#onSuccess()
 * succeeded} or {@link Acknowledgeable.AcknowledgementListener failed}.
 * 
 * <p>
 * In cases of worker or pipeline failure, this worker will nack all outstanding messages
 * automatically. (Note that this behavior is forced by some design choices Google made about the
 * Java PubSub client implementation, as opposed to a conscious design decision on the part of this
 * library.) As a result, some attempts to acknowledge or negatively acknowledge a message from this
 * worker in downstream tasks during pipeline failure may fail with an
 * {@link IllegalStateException}, indicating that the message had previously been acked (when trying
 * to nack) or nacked (when trying to ack).
 * 
 * <p>
 * This worker treats state and nacking as idempotent operations. So if a message is acked once,
 * then subsequent attempts to ack the message will receive the same result without actually
 * performing the ack operation again. The same is true for nacking.
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
 * the rate of messages being sent to it. If the downstream consumers are not able to keep up with
 * the rate of messages being sent to them and the subscriber is not configured with flow control,
 * then the message handling threads will start to block, which can lead to unpredictable results.
 * Users can recognize this situation by checking this task's {@link TaskManager.Metrics task
 * metrics} for high {@link TaskManager.Metrics#stalls()} or downstream tasks' metrics for high
 * {@link TaskManager.Metrics#pending() pending messages}.
 * 
 */
public class PubsubFirehoseProducerWorker implements FirehoseProducerWorker<Message<String>> {
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

  /**
   * The complexity in acking and nacking messages is in service of making those operations (a)
   * mutually exclusive and (b) idempotent. This is required because a PubSub subscriber will hang
   * forever if it is stopped while there are outstanding messages, so this worker has to nack all
   * outstanding messages when it is stopped. This means that the ack and nack operations have to be
   * thread-safe, since downstream tasks may be acking or nacking messages while the worker is
   * nacking during cleanup.
   */
  private class PubsubFirehoseMessage implements Message<String> {
    private static final int NONE = 0;
    private static final int ACKING = 10;
    private static final int ACKED = 11;
    private static final int NACKING = 20;
    private static final int NACKED = 21;

    private final AtomicInteger state = new AtomicInteger(NONE);
    private final PubsubMessage message;
    private final AckReplyConsumerWithResponse acker;
    private volatile Throwable failureCause = null;

    public PubsubFirehoseMessage(PubsubMessage message, AckReplyConsumerWithResponse acker) {
      this.message = requireNonNull(message, "message");
      this.acker = requireNonNull(acker, "acker");
    }

    @Override
    public String id() {
      return message.getMessageId();
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
      final int witness = state.compareAndExchange(NONE, ACKING);
      switch (witness) {
        case NONE:
          outstanding.remove(this);
          final ApiFuture<AckResponse> future = acker.ack();
          ApiFutures.addCallback(future, new ApiFutureCallback<AckResponse>() {
            @Override
            public void onSuccess(AckResponse result) {
              if (result == AckResponse.SUCCESSFUL) {
                synchronized (PubsubFirehoseMessage.this) {
                  state.set(ACKED);
                  PubsubFirehoseMessage.this.notifyAll();
                }
                listener.onSuccess();
              } else {
                onFailure(new IOException("ack failed with response " + result));
              }
            }

            @Override
            public void onFailure(Throwable t) {
              synchronized (PubsubFirehoseMessage.this) {
                state.set(ACKED);
                failureCause = t;
                PubsubFirehoseMessage.this.notifyAll();
              }
              listener.onFailure(t);
            }
          }, MoreExecutors.directExecutor());
          break;
        case ACKING:
          synchronized (PubsubFirehoseMessage.this) {
            int thestate = state.get();
            while (thestate == ACKING) {
              try {
                PubsubFirehoseMessage.this.wait();
              } catch (InterruptedException e) {
                // This is unfortunate. This breaks the perfect idempotency of the nack operation.
                // However, there isn't really anything else we can do here...
                Thread.currentThread().interrupt();
                listener.onFailure(e);
                return;
              }
              thestate = state.get();
            }
            assert thestate == ACKED;
          }
          // Fall through...
        case ACKED:
          // already acked, be idempotent
          if (failureCause == null)
            listener.onSuccess();
          else
            listener.onFailure(failureCause);
          break;
        case NACKING:
        case NACKED:
          // already nacked, we're in a bad state
          listener.onFailure(new IllegalStateException("message already nacked"));
          break;
        default:
          throw new AssertionError("unexpected state state: " + witness);
      }
    }

    @Override
    public void nack(AcknowledgementListener listener) {
      final int witness = state.compareAndExchange(NONE, NACKING);
      switch (witness) {
        case NONE:
          outstanding.remove(this);
          final ApiFuture<AckResponse> future = acker.nack();
          ApiFutures.addCallback(future, new ApiFutureCallback<AckResponse>() {
            @Override
            public void onSuccess(AckResponse result) {
              if (result == AckResponse.SUCCESSFUL) {
                synchronized (PubsubFirehoseMessage.this) {
                  state.set(NACKED);
                  PubsubFirehoseMessage.this.notifyAll();
                }
                listener.onSuccess();
              } else {
                onFailure(new IOException("nack failed with response " + result));
              }
            }

            @Override
            public void onFailure(Throwable t) {
              synchronized (PubsubFirehoseMessage.this) {
                state.set(NACKED);
                failureCause = t;
                PubsubFirehoseMessage.this.notifyAll();
              }
              listener.onFailure(t);
            }
          }, MoreExecutors.directExecutor());
          break;
        case NACKING:
          synchronized (PubsubFirehoseMessage.this) {
            int thestate = state.get();
            while (thestate == NACKING) {
              try {
                PubsubFirehoseMessage.this.wait();
              } catch (InterruptedException e) {
                // This is unfortunate. This breaks the perfect idempotency of the nack operation.
                // However, there isn't really anything else we can do here...
                Thread.currentThread().interrupt();
                listener.onFailure(e);
                return;
              }
              thestate = state.get();
            }
            assert thestate == NACKED;
          }
          // Fall through...
        case NACKED:
          // already acked, be idempotent
          if (failureCause == null)
            listener.onSuccess();
          else
            listener.onFailure(failureCause);
          break;
        case ACKING:
        case ACKED:
          // already nacked, be idempotent
          listener.onSuccess();
          break;
        default:
          throw new AssertionError("unexpected state state: " + witness);
      }
    }

    public int hashCode() {
      return System.identityHashCode(this);
    }

    public boolean equals(Object that) {
      return this == that;
    }
  }

  private final Set<PubsubFirehoseMessage> outstanding =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final AtomicLong receivedMetric = new AtomicLong(0);
  private final SubscriberFactory subscriberFactory;

  public PubsubFirehoseProducerWorker(SubscriberFactory subscriberFactory) {
    this.subscriberFactory = requireNonNull(subscriberFactory, "subscriberFactory");
  }

  @Override
  public void produce(Sink<Message<String>> sink) throws IOException, InterruptedException {
    try {
      final BlockingQueue<Throwable> failureCauses = new ArrayBlockingQueue<>(1);

      final Subscriber subscriber =
          subscriberFactory.newSubscriber(new MessageReceiverWithAckResponse() {
            @Override
            public void receiveMessage(PubsubMessage message,
                AckReplyConsumerWithResponse consumer) {
              final PubsubFirehoseMessage m = new PubsubFirehoseMessage(message, consumer);

              if (LOGGER.isTraceEnabled()) {
                // Let's guard this specific logging call with a check to avoid spamming the
                // System.identityHashCode() method for every message needlessly
                LOGGER.atTrace().addKeyValue("identity", m.hashCode())
                    .log("Received message from PubSub");
              }

              outstanding.add(m);
              try {
                // Note that this is (potentially) a blocking call. If the downstream sink is full,
                // then this will block until the sink is able to accept the message. This producer
                // is designed to be used with either (a) consumers that can keep up, (b) sinks that
                // don't block, or (c) backpressure using configured flow control on the subscriber.
                // If users find this call blocking, then they should consider either (a) tuning the
                // downstream consumers to keep up, e.g., increasing the worker count; (b) using a
                // sink that doesn't block, e.g., a queue with an unbounded capacity; or (c)
                // configuring tighter flow control on the subscriber.
                sink.put(m);
                receivedMetric.incrementAndGet();
              } catch (InterruptedException e) {
                outstanding.remove(m);
                Thread.currentThread().interrupt();
                LOGGER.atWarn().setCause(e)
                    .log("Interrupted while trying to put message. Stopping...");
                failureCauses.offer(e);
              } catch (Throwable e) {
                outstanding.remove(m);
                LOGGER.atError().setCause(e).log("Failed to put message. Stopping...");
                failureCauses.offer(e);
              }
            }
          });

      subscriber.addListener(new Subscriber.Listener() {
        @Override
        public void stopping(State from) {
          LOGGER.atDebug().addKeyValue("from", from).log("Subscriber stopping");
        }

        @Override
        public void terminated(State from) {
          LOGGER.atDebug().addKeyValue("from", from).log("Subscriber terminated");
        }

        @Override
        public void failed(Subscriber.State from, Throwable failure) {
          LOGGER.atError().setCause(failure).addKeyValue("from", from).log("Subscriber failed");
          failureCauses.offer(failure);
        }
      }, MoreExecutors.directExecutor());

      subscriber.startAsync().awaitRunning();
      try {
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedException();

        LOGGER.atInfo().log("Subscriber connected");

        Throwable failureCause = failureCauses.take();
        if (failureCause instanceof Error e)
          throw e;
        if (failureCause instanceof InterruptedException e)
          throw e;
        if (failureCause instanceof Exception e)
          throw new ExecutionException(e);
        throw new AssertionError("Unexpected error", failureCause);
      } finally {
        try {
          LOGGER.atDebug().log("Stopping subscriber");

          subscriber.stopAsync();

          // So this looks super funny. In a perfect world, we'd be able to call a method on the
          // subscriber to stop it, and it would just stop. Maybe subsequent acks or nacks would
          // fail, but that would be fine. However, Google's PubSub library doesn't work that way.
          // Instead, it will just hang forever until all outstanding messages are acked or
          // nacked. Neat! So we need to keep a reference to all outstanding messages, and then nack
          // them all when we stop the subscriber. Cool, right? So we do that here. That way, this
          // worker always stops, even if downstream tasks don't ack or nack messages.
          nackOutstandingMessages();

          subscriber.awaitTerminated();
          if (Thread.currentThread().isInterrupted())
            throw new InterruptedException();

          LOGGER.atDebug().log("Subscriber stopped");
        } catch (IllegalStateException e) {
          LOGGER.atWarn().setCause(e)
              .log("Failed to stop subscriber because subscriber was already stopped. Ignoring...");
        }
      }
    } catch (InterruptedException e) {
      LOGGER.atError().setCause(e).log("Pubsub firehose interrupted. Propagating...");
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    } catch (RuntimeException e) {
      LOGGER.atError().setCause(e).log("Pubsub firehose failed. Failing task...");
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

  /**
   * Nacks all outstanding messages. This is called when the worker is stopped to ensure that the
   * subscriber doesn't hang forever waiting for all messages to be acked or nacked. Which it will
   * absolutely do if we don't do ensure that all outstanding messages are acked or nacked.
   */
  private void nackOutstandingMessages() {
    while (!outstanding.isEmpty()) {
      LOGGER.atDebug().log("Nacking outstanding messages...");
      final Iterator<PubsubFirehoseMessage> iterator = outstanding.iterator();
      while (iterator.hasNext()) {
        final PubsubFirehoseMessage message = iterator.next();
        message.nack(new AcknowledgementListener() {
          @Override
          public void onSuccess() {
            LOGGER.atTrace().addKeyValue("identity", message.hashCode())
                .log("Successfully nacked message at stop time");
          }

          @Override
          public void onFailure(Throwable t) {
            LOGGER.atTrace().setCause(t).addKeyValue("identity", message.hashCode())
                .log("Failed to nack message at stop time");
          }
        });
        iterator.remove();
      }
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
