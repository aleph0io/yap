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

import io.aleph0.yap.messaging.core.Acknowledgeable.AcknowledgementListener;

/**
 * Allows the user to customize the behavior of the worker when an acknowledgement fails.
 */
@FunctionalInterface
public interface AcknowledgementFailureHandler {
  /**
   * Returns the default {@link AcknowledgementFailureHandler}, which simply re-throws the given
   * failure cause.
   * 
   * @return the default {@link AcknowledgementFailureHandler}
   */
  public static AcknowledgementFailureHandler defaultAcknowledgementFailureHandler() {
    return t -> {
      throw t;
    };
  }

  /**
   * Called when an {@link Acknowledgeable#ack(AcknowledgementListener) ack} fails. If the user
   * wants the task to continue, then they should simply return. If the user wants the task to fail,
   * then they should throw a {@link Throwable}, either the given one or a new one.
   * 
   * <p>
   * The underlying implementation handles {@link InterruptedException} and {@link Error} instances
   * for correctness, so those causes cannot be intercepted. Also, as a rule, users should not throw
   * these types from this method.
   * 
   * @param t the failure cause
   * @throws Throwable the failure cause, if the user wants the task to fail
   */
  void onAckFailure(Throwable t) throws Throwable;
}
