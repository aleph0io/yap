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

import io.aleph0.yap.core.Measureable;

public interface Topic<T> extends Measureable<Topic.Metrics>, AutoCloseable {
  public static record Metrics(long published, long stalls) {
    public Metrics {
      if (published < 0)
        throw new IllegalArgumentException("published must be at least zero");
      if (stalls < 0)
        throw new IllegalArgumentException("stalls must be at least zero");
    }
  }

  public void publish(T message) throws InterruptedException;

  @Override
  public void close();
}
