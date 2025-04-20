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
package io.aleph0.yap.core.transport;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import io.aleph0.yap.core.Measureable;

public interface Queue<T> extends Measureable<Queue.Metrics> {
  public static record Metrics(
      /**
       * The number of messages that are currently in the queue.
       */
      long pending,

      /**
       * The number of messages that have been produced to the queue.
       */
      long produced,

      /**
       * The number of times a publisher had to wait for space in the queue.
       */
      long stalls,

      /**
       * The number of messages that have been consumed from the queue.
       */
      long consumed,

      /**
       * The number of times a consumer has had to wait for a message to be available in the queue.
       */
      long waits) {
  }

  /**
   * Attempts to receive a message from the queue. This method will return immediately, even if
   * there are no messages available in the queue. Returns {@code null} if the queue is empty,
   * whethere it is closed or not, so users must not conclude that the queue is closed and drained
   * from this method returning {@code null}.
   * 
   * @return the message received from the queue, or {@code null} if the queue is empty
   */
  public T tryReceive();

  /**
   * Receives a message from the queue, waiting if necessary. This method will block until a message
   * is received from the queue or the timeout expires. If the queue is closed and drained, this
   * method will return {@code null}. Users may conclude that the queue is closed and drained from
   * this method returning {@code null}.
   * 
   * @param timeout the maximum time to wait for a message to be received
   * @return the message received from the queue, or {@code null} if the queue is closed and drained
   * @throws InterruptedException if the thread is interrupted while waiting
   * @throws TimeoutException if the wait times out before a message is received
   */
  public T receive(Duration timeout) throws InterruptedException, TimeoutException;

  /**
   * Receives a message from the queue, waiting if necessary. This method will block until a message
   * is received from the queue. If the queue is closed and drained, this method will return
   * {@code null}. Users may conclude that the queue is closed and drained from this method
   * returning {@code null}.
   * 
   * @return the message received from the queue, or {@code null} if the queue is closed and drained
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public T receive() throws InterruptedException;

  public boolean isDrained();
}
