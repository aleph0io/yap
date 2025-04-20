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
package io.aleph0.yap.core;

import io.aleph0.yap.core.task.TaskController;

public interface Measureable<M> {
  /**
   * Non-destructive check of the metrics. This should be used to check the state of metrics without
   * clearing them, for example in a {@link TaskController}.
   * 
   * @return the metrics
   */
  public M checkMetrics();

  /**
   * Destructive read of the metrics. This should be used to check and reset the state of metrics,
   * for example by the metrics checking thread.
   *  
   * @return the metrics
   */
  public M flushMetrics();
}
