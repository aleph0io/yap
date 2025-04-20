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
import java.util.List;
import java.util.concurrent.ExecutionException;
import io.aleph0.yap.core.task.action.TaskAction;
import io.aleph0.yap.core.transport.Queue;
import io.aleph0.yap.core.transport.Topic;

/**
 * The TaskController is responsible for controlling the lifecycle of a task. The
 * {@link TaskManager} will call the methods of this interface to notify the TaskController of
 * various events that occur during the lifecycle of a task, and the TaskController will return the
 * list of {@link TaskAction}s to perform in response to those events. The TaskController is
 * generally free to decide how to respond to these events, for example implementing worker-level
 * retry logic or implementing a circuit-breaker pattern, but to ensure correctness, it must conform
 * to the high-level task lifecycle defined by the {@code TaskManager}.
 */
public interface TaskController {
  public static interface ProducerTaskControllerBuilder<OutputT> {
    public TaskController build(Topic<OutputT> topic);
  }

  public static interface ProcessorTaskControllerBuilder<InputT, OutputT> {
    public TaskController build(Queue<InputT> queue, Topic<OutputT> topi);
  }

  public static interface ConsumerTaskControllerBuilder<InputT> {
    public TaskController build(Queue<InputT> queue);
  }

  /**
   * Called when a task has been started. Called as the first event only.
   * 
   * @return the next actions to perform
   */
  public List<TaskAction> onTaskStart();

  /**
   * Called when a worker was started successfully in response to a start request.
   * 
   * @param id the id of the worker that was started
   * @return the next actions to perform
   * 
   * @see TaskAction#newStartWorkerTaskAction()
   */
  public List<TaskAction> onWorkerStarted(int id);

  /**
   * Called when a worker was stopped in response to a stop request.
   * 
   * @param id the id of the worker that was stopped
   * @return the next action to perform
   * 
   * @see TaskAction#newStopWorkerTaskAction()
   */
  public List<TaskAction> onWorkerStopped(int id);

  /**
   * Called when a worker completed normally, for example because all input messages were processed.
   * 
   * @param id the id of the worker that completed
   * @return the next action to perform
   */
  public List<TaskAction> onWorkerCompletedNormally(int id);

  /**
   * Called when a worker completed exceptionally by propagating an exception.
   * 
   * @param id the id of the worker that completed
   * @param e the exception that caused the worker to complete exceptionally
   * 
   * @return the next action to perform
   */
  public List<TaskAction> onWorkerCompletedExceptionally(int id, Throwable e);

  /**
   * Called when the user has requested to cancel this task. In general, this means that the
   * controller should stop all running workers and finish the task with an appropriate status as
   * soon as possible.
   * 
   * @return the next action to perform
   * 
   * @see TaskAction#newStopWorkerTaskAction()
   * @see TaskAction#newCancelTaskAction()
   * @see TaskAction#newSucceedTask()
   * @see TaskAction#newFailTask(Exception)
   */
  public List<TaskAction> onCancelRequested();

  /**
   * Called when no lifecycle events have occurred within {@link #getHeartbeatInterval() the
   * heartbeat interval}. This ensures that the controller has periodic access to execute actions
   * even if no lifecycle events are occurring, for example to start more workers in response to a
   * backlog of messages, or stop workers in response to no backlog.
   * 
   * @return the next action to perform
   */
  public List<TaskAction> onHeartbeat();

  /**
   * The maximum amount of time to wait for a lifecycle event, after which {@link #onHeartbeat()}
   * will be called.
   */
  public Duration getHeartbeatInterval();

  /**
   * Called when the task has been successfully completed. This is the last event that will be
   * called.
   */
  public void onTaskSucceeded();

  /**
   * Called when the task has been cancelled. This is the last event that will be called.
   */
  public void onTaskCancelled();

  /**
   * Called when the task has failed. This is the last event that will be called.
   * 
   * @param e the exception that caused the task to fail
   */
  public void onTaskFailed(ExecutionException e);
}
