/*-
 * =================================LICENSE_START==================================
 * yap-core
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
package io.aleph0.yap.core.transport.queue;

import static java.util.Collections.unmodifiableList;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.build.QueueBuilder;
import io.aleph0.yap.core.transport.Channel;
import io.aleph0.yap.core.transport.Queue;

public class DefaultQueue<T> implements Queue<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultQueue.class);

  public static record Metrics(int depth, long sent, long blocks, long received, long waits) {
    public Metrics {
      if (depth < 0)
        throw new IllegalArgumentException("depth must be greater than or equal to 0");
      if (sent < 0)
        throw new IllegalArgumentException("sent must be greater than or equal to 0");
      if (blocks < 0)
        throw new IllegalArgumentException("blocks must be greater than or equal to 0");
      if (received < 0)
        throw new IllegalArgumentException("received must be greater than or equal to 0");
      if (waits < 0)
        throw new IllegalArgumentException("waits must be greater than or equal to 0");
    }
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> implements QueueBuilder<T> {
    private int capacity = 100;

    public Builder<T> setCapacity(int capacity) {
      this.capacity = capacity;
      return this;
    }

    @Override
    public Queue<T> build(List<Channel<T>> subscriptions) {
      return new DefaultQueue<>(capacity, subscriptions);
    }
  }

  private final java.util.Queue<T> queue;
  private final List<Channel<T>> subscriptions;

  /**
   * Lock for synchronizing access to the deque.
   */
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Condition signaled when the deque has been made not empty.
   */
  private final Condition notEmpty = lock.newCondition();

  /**
   * Condition signaled when the deque has been made not full.
   */
  private final Condition notFull = lock.newCondition();

  private int numSubscribers;

  private int closedSubscribers = 0;

  private final AtomicLong produced = new AtomicLong(0);

  private final AtomicLong stalls = new AtomicLong(0);

  private final AtomicLong consumed = new AtomicLong(0);

  private final AtomicLong waits = new AtomicLong(0);


  public DefaultQueue(int capacity, List<Channel<T>> subscriptions) {
    this.queue = new ArrayDeque<>(capacity);
    this.subscriptions = unmodifiableList(subscriptions);
    this.numSubscribers = subscriptions.size();
    for (Channel<T> subscription : subscriptions) {
      subscription.bind(new io.aleph0.yap.core.transport.Channel.Binding<>() {
        private volatile boolean closed = false;

        @Override
        public boolean tryPublish(T message) {
          boolean result;

          lock.lock();
          try {
            if (closed)
              throw new IllegalStateException("closed");
            if (queue.size() < capacity)
              result = queue.offer(message);
            else
              result = false;
            if (result) {
              produced.incrementAndGet();
              notEmpty.signalAll();
            }
          } finally {
            lock.unlock();
          }

          LOGGER.atDebug().addKeyValue("message", message).addKeyValue("result", result)
              .log("tryPublish");

          return result;
        }

        @Override
        public void publish(T message) throws InterruptedException {
          lock.lock();
          try {
            if (closed)
              throw new IllegalStateException("closed");

            boolean delivered;
            if (queue.size() < capacity)
              delivered = queue.offer(message);
            else
              delivered = false;

            if (!delivered) {
              stalls.incrementAndGet();
              do {
                if (queue.size() < capacity)
                  delivered = queue.offer(message);
                else
                  notFull.await();
              } while (!delivered && !closed);
            }

            if (delivered) {
              produced.incrementAndGet();
              notEmpty.signalAll();
            } else {
              throw new IllegalStateException("closed");
            }
          } finally {
            lock.unlock();
          }
          LOGGER.atDebug().addKeyValue("message", message).log("publish");
        }

        @Override
        public void close() {
          lock.lock();
          try {
            if (closed == false) {
              closed = true;
              closedSubscribers = closedSubscribers + 1;
              notEmpty.signalAll();
              notFull.signalAll();
            }
          } finally {
            lock.unlock();
          }
        }
      });
    }

  }

  @Override
  public T tryReceive() {
    T result;

    lock.lock();
    try {
      result = queue.poll();
      if (result != null) {
        consumed.incrementAndGet();
        notFull.signalAll();
      }
    } finally {
      lock.unlock();
    }

    LOGGER.atDebug().addKeyValue("result", result)
        .addKeyValue("closedSubscribers", closedSubscribers)
        .addKeyValue("numSubscribers", numSubscribers).log("tryReceive");

    return result;
  }

  @Override
  public T receive(Duration timeout) throws InterruptedException, TimeoutException {
    final long expiration = System.nanoTime() + timeout.toNanos();

    T result;
    lock.lock();
    try {
      result = queue.poll();
      if (result == null && closedSubscribers != numSubscribers) {
        waits.incrementAndGet();
        long remaining = expiration - System.nanoTime();
        if (remaining <= 0)
          throw new TimeoutException();
        do {
          if (result == null)
            remaining = notEmpty.awaitNanos(remaining);
          if (remaining > 0)
            result = queue.poll();
        } while (result == null && closedSubscribers != numSubscribers && remaining > 0);
        if (result == null && remaining <= 0)
          throw new TimeoutException();
      }
      if (result != null) {
        consumed.incrementAndGet();
        notFull.signalAll();
      }
    } finally {
      lock.unlock();
    }

    LOGGER.atDebug().addKeyValue("result", result)
        .addKeyValue("closedSubscribers", closedSubscribers)
        .addKeyValue("numSubscribers", numSubscribers).log("receive");

    return result;
  }

  @Override
  public T receive() throws InterruptedException {
    T result;

    lock.lock();
    try {
      result = queue.poll();
      if (result == null && closedSubscribers != numSubscribers) {
        waits.incrementAndGet();
        do {
          if (result == null)
            notEmpty.await();
          result = queue.poll();
        } while (result == null && closedSubscribers != numSubscribers);
      }
      if (result != null) {
        consumed.incrementAndGet();
        notFull.signalAll();
      }
    } finally {
      lock.unlock();
    }

    LOGGER.atDebug().addKeyValue("result", result)
        .addKeyValue("closedSubscribers", closedSubscribers)
        .addKeyValue("numSubscribers", numSubscribers).log("receive");

    return result;
  }

  @Override
  public boolean isDrained() {
    lock.lock();
    try {
      return queue.isEmpty() && closedSubscribers == subscriptions.size();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Queue.Metrics checkMetrics() {
    final long pending = queue.size();
    final long produced = this.produced.get();
    final long stalls = this.stalls.get();
    final long consumed = this.consumed.get();
    final long waits = this.waits.get();
    return new Queue.Metrics(pending, produced, stalls, consumed, waits);
  }

  @Override
  public Queue.Metrics flushMetrics() {
    final Queue.Metrics metrics = checkMetrics();
    this.produced.set(0);
    this.stalls.set(0);
    this.consumed.set(0);
    this.waits.set(0);
    return metrics;
  }
}
