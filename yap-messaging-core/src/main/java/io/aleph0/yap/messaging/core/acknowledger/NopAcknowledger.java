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
package io.aleph0.yap.messaging.core.acknowledger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import io.aleph0.yap.messaging.core.Acknowledgeable;
import io.aleph0.yap.messaging.core.Acknowledger;
import io.aleph0.yap.messaging.core.AcknowledgerMetrics;

/**
 * An implementation of {@link Acknowledger} that does not perform any acknowledgement. This is
 * useful in contexts where acknowledgement is not necessary or undesired, e.g., in pipelines with
 * fanout where exactly one branch will handle each message, so acknowledging dropped messages on
 * other branches would be incorrect.
 * 
 * @param <T> the type of {@link Acknowledgeable} that this acknowledger will handle
 */
public class NopAcknowledger<T extends Acknowledgeable> implements Acknowledger<T> {
  private final AtomicLong counter = new AtomicLong(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  @Override
  public void acknowledge(Acknowledgeable acknowledgeable) {
    if (closed.get())
      throw new IllegalStateException("closed");
    counter.incrementAndGet();
  }

  @Override
  public void close() throws InterruptedException, ExecutionException {
    closed.set(true);
  }

  @Override
  public void throwIfPresent() throws InterruptedException, ExecutionException {
    // We do nothing, so we never fail, so this method is a NOP.
  }

  @Override
  public AcknowledgerMetrics checkMetrics() {
    final long acknowledged = counter.get();
    final long retiredSuccess = acknowledged;
    final long retiredFailure = 0L;
    final long retired = acknowledged;
    final long awaiting = 0L;
    return new AcknowledgerMetrics(acknowledged, retired, retiredSuccess, retiredFailure, awaiting);
  }

  @Override
  public AcknowledgerMetrics flushMetrics() {
    final AcknowledgerMetrics metrics = checkMetrics();
    counter.set(0);
    return metrics;
  }
}
