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
package io.aleph0.yap.core.task;

import java.time.Duration;
import io.aleph0.yap.core.transport.Queue;
import io.aleph0.yap.core.transport.Topic;

public final class DefaultProcessorTaskController<InputT, OutputT>
    extends DefaultTaskController<InputT, OutputT> {
  public static <InputT, OutputT> Builder<InputT, OutputT> builder() {
    return new Builder<>();
  }

  public static class Builder<InputT, OutputT>
      implements TaskController.ProcessorTaskControllerBuilder<InputT, OutputT> {
    private int desiredConcurrency = 1;

    public Builder<InputT, OutputT> setDesiredConcurrency(int parallelism) {
      if (parallelism < 1)
        throw new IllegalArgumentException("desiredConcurrency must be at least 1");
      this.desiredConcurrency = parallelism;
      return this;
    }

    private Duration heartbeatInterval = Duration.ofSeconds(60);

    public Builder<InputT, OutputT> setHeartbeatInterval(Duration heartbeatInterval) {
      if (heartbeatInterval == null)
        throw new NullPointerException();
      if (!heartbeatInterval.isPositive())
        throw new IllegalArgumentException("heartbeatInterval must be positive");
      this.heartbeatInterval = heartbeatInterval;
      return this;
    }

    @Override
    public TaskController build(Queue<InputT> queue, Topic<OutputT> topic) {
      return new DefaultTaskController<>(desiredConcurrency, heartbeatInterval, queue, topic);
    }
  }

  public DefaultProcessorTaskController(int parallelism, Duration heartbeatInterval,
      Queue<InputT> queue, Topic<OutputT> topic) {
    super(parallelism, heartbeatInterval, queue, topic);
  }
}
