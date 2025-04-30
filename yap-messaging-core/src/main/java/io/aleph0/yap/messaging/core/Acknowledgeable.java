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
package io.aleph0.yap.messaging.core;

public interface Acknowledgeable {
  /**
   * A listener that is notified when an acknowledgement or negative acknowledgement is complete.
   */
  public static interface AcknowledgementListener {
    public void onSuccess();

    public void onFailure(Throwable t);
  }

  /**
   * Acknowledge this object. Generally used to indicate that the message has been processed
   * successfully and can safely be removed from its respective source.
   * 
   * <p>
   * This method guarantees that the given listener will be called exactly once, with either
   * {@link AcknowledgementListener#onSuccess() success} or
   * {@link AcknowledgementListener#onFailure(Throwable) failure}.
   * 
   * <p>
   * This operation is idempotent, meaning that calling it multiple times will have the same effect
   * as calling it once.
   * 
   * <p>
   * This method is mutually exclusive with {@link #nack(AcknowledgementListener) nack}, meaning
   * that if this method is called, then a subsequent call to the nack method should fail.
   * 
   * @param listener the listener to notify when the acknowledgement is complete, either
   *        successfully or with an error.
   */
  public void ack(AcknowledgementListener listener);

  /**
   * Negatively acknowledge this object. Generally used to indicate that there was an error
   * processing the message, and it should be retried according to the semantics of its respective
   * source.
   * 
   * <p>
   * This method guarantees that the given listener will be called exactly once, with either
   * {@link AcknowledgementListener#onSuccess() success} or
   * {@link AcknowledgementListener#onFailure(Throwable) failure}.
   * 
   * <p>
   * This operation is idempotent, meaning that calling it multiple times will have the same effect
   * as calling it once.
   * 
   * <p>
   * This method is mutually exclusive with {@link #ack(AcknowledgementListener) ack}, meaning that
   * if this method is called, then a subsequent call to the ack method should fail.
   * 
   * @param listener the listener to notify when the negative acknowledgement is complete, either
   *        successfully or with an error.
   */
  public void nack(AcknowledgementListener listener);
}
