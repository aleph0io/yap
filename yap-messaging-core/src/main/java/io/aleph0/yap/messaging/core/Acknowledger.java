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

import java.util.concurrent.ExecutionException;
import io.aleph0.yap.core.Measureable;

/**
 * An Acknowledger is responsible for acknowledging messages that have been completely processed. It
 * provides methods for acknowledging messages, closing the acknowledger, and checking for any
 * failures that may have occurred during acknowledgement. The Acknowledger interface extends the
 * Measureable interface, allowing it to provide metrics about its performance and behavior.
 * 
 * <p>
 * The {@link #throwIfPresent()} method should be called periodically to check for any failures that
 * may have occurred during acknowledgement, and to throw any unhandled exceptions that may have
 * been suppressed by the {@link AcknowledgementFailureHandler}.
 *
 * <p>
 * The acknowledger should be {@link #close() closed} when it is no longer needed, to ensure that
 * any resources it holds are released and that any outstanding acknowledgements are completed.
 * 
 * @param <T> the type of Acknowledgeable objects that this Acknowledger can acknowledge.
 */
public interface Acknowledger<T extends Acknowledgeable>
    extends Measureable<AcknowledgerMetrics>, AutoCloseable {
  /**
   * Acknowledges the given {@link Acknowledgeable} object. This method is asynchronous and may
   * throw an exception if the acknowledgement fails. The exact behavior of this method will depend
   * on the implementation, but it should generally ensure that the acknowledgement is processed
   * correctly and any necessary resources are released.
   * 
   * @param acknowledgeable
   */
  void acknowledge(Acknowledgeable acknowledgeable);

  /**
   * Checks if there is a failure cause present, and if so, throws it. Because acknowledgement is
   * asynchronous, this method should be called periodically during processing to check for
   * failures. Note that any exceptions suppressed by the {@link AcknowledgementFailureHandler} will
   * not be thrown by this method, as they are considered to be handled by the user. Only unhandled
   * exceptions will be thrown.
   * 
   * @throws Error if an error occurred during acknowledgement.
   * @throws InterruptedException if the thread was interrupted while waiting for acknowledgements
   *         to complete.
   * @throws ExecutionException if an exception occurred during acknowledgement that was not handled
   *         by the {@link AcknowledgementFailureHandler}. The cause of the exception will be the
   *         original exception that occurred during acknowledgement.
   */
  void throwIfPresent() throws InterruptedException, ExecutionException;

  /**
   * Closes the acknowledger, preventing any new acknowledgements from being initiated. This method
   * will wait for all outstanding acknowledgements to complete before returning. If any
   * acknowledgements fail during this time, the method will throw an exception. If the acknowledger
   * is already closed, this method will do nothing.
   * 
   * @throws InterruptedException if the thread was interrupted while waiting for acknowledgements
   *         to complete.
   * @throws ExecutionException if an exception occurred during acknowledgement that was not handled
   *         by the {@link AcknowledgementFailureHandler}. The cause of the exception will be the
   *         original exception that occurred during acknowledgement.
   * 
   * @see #throwIfPresent() for more details on how exceptions are handled during acknowledgement.
   */
  void close() throws InterruptedException, ExecutionException;

  AcknowledgerMetrics checkMetrics();

  AcknowledgerMetrics flushMetrics();

}
