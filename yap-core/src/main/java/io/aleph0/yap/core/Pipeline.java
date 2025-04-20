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

import static java.util.Objects.requireNonNull;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import io.aleph0.yap.core.build.PipelineBuilder;
import io.aleph0.yap.core.pipeline.PipelineManager.PipelinePhase;
import io.aleph0.yap.core.pipeline.PipelineManager.PipelineState;
import io.aleph0.yap.core.task.TaskManager;

public interface Pipeline extends Measureable<Pipeline.Metrics> {
  public static record Metrics(
      /**
       * The pipeline ID
       */
      int id,

      /**
       * The current phase of the pipeline
       */
      PipelinePhase phase,

      /**
       * The current state of the pipeline
       */
      PipelineState state,

      /**
       * The metrics for all tasks
       */
      Map<String, TaskManager.Metrics<?>> tasks) {
    public Metrics {
      requireNonNull(phase);
      requireNonNull(state);
      requireNonNull(tasks);
    }
  }


  public static PipelineBuilder builder() {
    return new PipelineBuilder();
  }

  public static interface LifecycleListener {
    default void onPipelineStarted(int pipeline) {}

    default void onPipelineTaskStarted(int pipeline, String task) {}

    default void onPipelineTaskWorkerStarted(int pipeline, String task, int worker) {}

    default void onPipelineTaskWorkerStopRequested(int pipeline, String task, int worker) {}

    default void onPipelineTaskWorkerStopped(int pipeline, String task, int worker) {}

    default void onPipelineTaskWorkerCompletedNormally(int pipeline, String task, int worker) {}

    default void onPipelineTaskWorkerCompletedExceptionally(int pipeline, String task, int worker,
        Throwable cause) {}

    default void onPipelineTaskCancelRequested(int pipeline, String task, int worker) {}

    default void onPipelineTaskCompleted(int pipeline, String task) {}

    default void onPipelineTaskCancelled(int pipeline, String task) {}

    default void onPipelineTaskFailed(int pipeline, String task, Throwable cause) {}

    default void onPipelineCancelRequested(int pipeline) {}

    default void onPipelineCompleted(int pipeline) {}

    default void onPipelineCancelled(int pipeline) {}

    default void onPipelineFailed(int pipeline, Throwable cause) {}
  }

  public void addLifecycleListener(LifecycleListener listener);

  public void removeLifecycleListener(LifecycleListener listener);

  public int getId();

  public void start();

  public void cancel();

  public void await() throws InterruptedException, ExecutionException, CancellationException;
}
