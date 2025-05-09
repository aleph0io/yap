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

public interface Channel<T> extends AutoCloseable {
  /**
   * A destination for messages. This is a simple interface that allows the channel to deliver
   * messages to a concrete destination.
   * 
   * @param <T>
   */
  interface Binding<T> {
    /**
     * Attempts to deliver the message immediately. Must not block. If the message was delivered
     * successfully, then returns true. Otherwise, returns false.
     * 
     * @param message the message to publish
     * @return true if the message was published, false otherwise
     */
    public boolean tryPublish(T message);

    /**
     * Delivers the message, blocking if necessary. This method must not return until the message is
     * delivered.
     * 
     * @param message the message to publish
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void publish(T message) throws InterruptedException;

    /**
     * Closes the binding. No more messages will be delivered after this method is called. Any
     * threads currently blocking while trying to publish a message will be interrupted. When this
     * method returns, all blocking threads will have been interrupted, but not joined.
     */
    public void close();
  }

  void bind(Channel.Binding<T> binding);

  boolean tryPublish(T message);

  void publish(T message) throws InterruptedException;

  @Override
  void close();
}
