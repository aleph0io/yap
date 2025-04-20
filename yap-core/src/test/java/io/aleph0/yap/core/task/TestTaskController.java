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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import io.aleph0.yap.core.task.action.TaskAction;

public class TestTaskController implements TaskController {
  private final int desiredConcurrency;
  private int starting = 0;
  private int running = 0;
  private ExecutionException failureCause = null;
  private boolean canceling = false;
  private boolean failing = false;

  public TestTaskController(int desiredConcurrency) {
    this.desiredConcurrency = desiredConcurrency;
  }

  @Override
  public List<TaskAction> onTaskStart() {
    List<TaskAction> actions = new ArrayList<>();
    for (int i = 0; i < desiredConcurrency; i++) {
      starting = starting + 1;
      actions.add(TaskAction.newStartWorkerTaskAction());
    }
    return actions;
  }

  @Override
  public List<TaskAction> onWorkerStarted(int id) {
    starting = starting - 1;
    running = running + 1;
    if (canceling)
      return List.of(TaskAction.newStopWorkerTaskAction());
    return List.of();
  }

  @Override
  public List<TaskAction> onWorkerStopped(int id) {
    if (!canceling)
      throw new IllegalStateException("Worker " + id + " stopped, but not canceling");
    running = running - 1;
    if (starting == 0 && running == 0 && failing)
      return List.of(TaskAction.newFailTask(failureCause));
    if (starting == 0 && running == 0)
      return List.of(TaskAction.newCancelTask());
    return List.of();
  }

  @Override
  public List<TaskAction> onWorkerCompletedNormally(int id) {
    running = running - 1;
    if (starting == 0 && running == 0 && failing)
      return List.of(TaskAction.newFailTask(failureCause));
    if (starting == 0 && running == 0 && canceling)
      return List.of(TaskAction.newCancelTask());
    if (starting == 0 && running == 0)
      return List.of(TaskAction.newSucceedTask());
    return List.of();
  }

  @Override
  public List<TaskAction> onWorkerCompletedExceptionally(int id, Throwable e) {
    running = running - 1;
    failureCause = new ExecutionException(e);
    failing = true;

    if (starting == 0 && running == 0)
      return List.of(TaskAction.newFailTask(failureCause));

    List<TaskAction> actions = new ArrayList<>();
    for (int i = 0; i < running; i++)
      actions.add(TaskAction.newStopWorkerTaskAction());

    return actions;
  }

  @Override
  public List<TaskAction> onCancelRequested() {
    canceling = true;

    if (starting == 0 && running == 0)
      return List.of(TaskAction.newCancelTask());

    List<TaskAction> actions = new ArrayList<>();
    for (int i = 0; i < running; i++)
      actions.add(TaskAction.newStopWorkerTaskAction());

    return actions;
  }

  @Override
  public List<TaskAction> onHeartbeat() {
    // NOP
    return List.of();
  }

  @Override
  public Duration getHeartbeatInterval() {
    return Duration.ofMinutes(1L);
  }

  @Override
  public void onTaskSucceeded() {}

  @Override
  public void onTaskCancelled() {}

  @Override
  public void onTaskFailed(ExecutionException e) {}
}
