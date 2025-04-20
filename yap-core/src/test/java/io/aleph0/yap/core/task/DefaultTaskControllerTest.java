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

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.aleph0.yap.core.task.DefaultTaskController;
import io.aleph0.yap.core.task.DefaultTaskController.TaskState;
import io.aleph0.yap.core.task.action.CancelTaskAction;
import io.aleph0.yap.core.task.action.FailTaskAction;
import io.aleph0.yap.core.task.action.StartWorkerTaskAction;
import io.aleph0.yap.core.task.action.StopWorkerTaskAction;
import io.aleph0.yap.core.task.action.SucceedTaskAction;
import io.aleph0.yap.core.task.action.TaskAction;
import io.aleph0.yap.core.transport.Queue;
import io.aleph0.yap.core.transport.Topic;

public class DefaultTaskControllerTest {
  private DefaultTaskController<String, String> controller;
  private Queue<String> queue;
  private Topic<String> topic;
  private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(5);

  @BeforeEach
  void setupDefaultTaskControllerTest() {
    queue = null;
    topic = null;
    controller = new DefaultTaskController<>(1, HEARTBEAT_INTERVAL, queue, topic);
  }

  @Test
  void givenReadyState_whenTaskStarts_thenInitiatesDesiredWorkers() {
    // Given a controller in READY state with desired concurrency of 3
    controller = new DefaultTaskController<>(3, HEARTBEAT_INTERVAL, queue, topic);

    // When the task starts
    List<TaskAction> actions = controller.onTaskStart();

    // Then it should transition to RUNNING and start desired number of workers
    assertThat(controller.state).isEqualTo(TaskState.RUNNING);
    assertThat(actions).hasSize(3);
    assertThat(actions).isEqualTo(List.of(new StartWorkerTaskAction(), new StartWorkerTaskAction(),
        new StartWorkerTaskAction()));
  }

  @Test
  void givenRunningState_whenWorkerStarts_thenTrackRunningWorkers() {
    // Given a controller in RUNNING state
    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1).isEqualTo(List.of(new StartWorkerTaskAction()));

    // When a worker starts
    List<TaskAction> actions2 = controller.onWorkerStarted(1);

    // Then it should track the worker but take no actions
    assertThat(controller.workers).isEqualTo(1);
    assertThat(actions2).isEmpty();
  }

  @Test
  void givenRunningState_whenAllWorkersCompleteNormally_thenTransitionToCompleted() {
    // Given a running controller with a started worker
    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1).isEqualTo(List.of(new StartWorkerTaskAction()));

    List<TaskAction> actions2 = controller.onWorkerStarted(1);
    assertThat(actions2).isEmpty();

    // When the worker completes normally
    List<TaskAction> actions3 = controller.onWorkerCompletedNormally(1);

    // Then it should transition to COMPLETED and succeed the task
    assertThat(controller.state).isEqualTo(TaskState.COMPLETED);
    assertThat(actions3).isEqualTo(List.of(new SucceedTaskAction()));
  }

  @Test
  void givenRunningState_whenAWorkerCompletesExceptionally_thenTransitionToFailing() {
    // Given a controller with two workers
    controller = new DefaultTaskController<>(2, HEARTBEAT_INTERVAL, queue, topic);

    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1)
        .isEqualTo(List.of(new StartWorkerTaskAction(), new StartWorkerTaskAction()));

    List<TaskAction> actions2 = controller.onWorkerStarted(1);
    assertThat(actions2).isEmpty();

    List<TaskAction> actions3 = controller.onWorkerStarted(2);
    assertThat(actions3).isEmpty();

    // When one worker fails
    RuntimeException exception = new RuntimeException("Test exception");
    List<TaskAction> actions4 = controller.onWorkerCompletedExceptionally(1, exception);

    // Then it should transition to FAILING and stop all remaining workers
    assertThat(controller.state).isEqualTo(TaskState.FAILING);
    assertThat(actions4).isEqualTo(List.of(new StopWorkerTaskAction()));
    assertThat(controller.failureCause).isNotNull();
  }

  @Test
  void givenRunningState_whenCancelRequested_thenTransitionToCanceling() {
    // Given a controller with one worker
    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1).isEqualTo(List.of(new StartWorkerTaskAction()));

    List<TaskAction> actions2 = controller.onWorkerStarted(1);
    assertThat(actions2).isEmpty();

    // When cancellation is requested
    List<TaskAction> actions3 = controller.onCancelRequested();

    // Then it should transition to CANCELING and stop the worker
    assertThat(controller.state).isEqualTo(TaskState.CANCELING);
    assertThat(actions3).isEqualTo(List.of(new StopWorkerTaskAction()));
  }

  @Test
  void givenCancelingState_whenAllWorkersStopped_thenTransitionToCanceled() {
    // Given a canceling controller
    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1).isEqualTo(List.of(new StartWorkerTaskAction()));

    List<TaskAction> actions2 = controller.onWorkerStarted(1);
    assertThat(actions2).isEmpty();

    // When cancellation is requested
    List<TaskAction> actions3 = controller.onCancelRequested();
    assertThat(actions3).isEqualTo(List.of(new StopWorkerTaskAction()));

    // When the worker stops
    List<TaskAction> actions4 = controller.onWorkerStopped(1);

    // Then it should transition to CANCELED
    assertThat(controller.state).isEqualTo(TaskState.CANCELED);
    assertThat(actions4).isEqualTo(List.of(new CancelTaskAction()));
  }

  @Test
  void givenFailingState_whenAllWorkersComplete_thenTransitionToFailed() {
    // Given a failing controller
    controller = new DefaultTaskController<>(2, HEARTBEAT_INTERVAL, queue, topic);
    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1)
        .isEqualTo(List.of(new StartWorkerTaskAction(), new StartWorkerTaskAction()));

    List<TaskAction> actions2 = controller.onWorkerStarted(1);
    assertThat(actions2).isEmpty();

    List<TaskAction> actions3 = controller.onWorkerStarted(2);
    assertThat(actions3).isEmpty();


    RuntimeException exception = new RuntimeException("First exception");
    List<TaskAction> actions4 = controller.onWorkerCompletedExceptionally(1, exception);
    assertThat(actions4).isEqualTo(List.of(new StopWorkerTaskAction()));


    // When the last worker completes, we should transition to FAILED

    List<TaskAction> actions5 = controller.onWorkerCompletedNormally(2);
    assertThat(controller.state).isEqualTo(TaskState.FAILED);
    assertThat(actions5).hasSize(1);
    assertThat(actions5.get(0)).isInstanceOf(FailTaskAction.class);
    ExecutionException cause = ((FailTaskAction) actions5.get(0)).cause();

    assertThat(cause.getCause()).isExactlyInstanceOf(RuntimeException.class);
  }

  @Test
  void givenCompletingState_whenWorkerFails_thenTransitionToFailing() {
    // Given a controller in COMPLETING state
    controller = new DefaultTaskController<>(2, HEARTBEAT_INTERVAL, queue, topic);
    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1)
        .isEqualTo(List.of(new StartWorkerTaskAction(), new StartWorkerTaskAction()));

    List<TaskAction> actions2 = controller.onWorkerStarted(1);
    assertThat(actions2).isEmpty();

    List<TaskAction> actions3 = controller.onWorkerStarted(2);
    assertThat(actions3).isEmpty();

    controller.onWorkerCompletedNormally(1);

    // When a worker fails while in COMPLETING state
    RuntimeException exception = new RuntimeException("Test exception");
    List<TaskAction> actions = controller.onWorkerCompletedExceptionally(2, exception);

    // Then it should transition to FAILED since we now have no workers running
    assertThat(controller.state).isEqualTo(TaskState.FAILED);
    assertThat(actions).hasSize(1);
    assertThat(actions.get(0)).isInstanceOf(FailTaskAction.class);
    assertThat(controller.failureCause).isNotNull();
  }

  @Test
  void givenRunningState_whenHeartbeat_thenNoActionsTaken() {
    // Given a controller in RUNNING state
    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1).isEqualTo(List.of(new StartWorkerTaskAction()));

    List<TaskAction> actions2 = controller.onWorkerStarted(1);
    assertThat(actions2).isEmpty();

    // When a heartbeat occurs
    List<TaskAction> actions = controller.onHeartbeat();

    // Then no actions should be taken
    assertThat(actions).isEmpty();
  }

  @Test
  void givenMultipleFailures_whenAllWorkersComplete_thenCapturesBothExceptions() {
    // Given a controller with two workers
    controller = new DefaultTaskController<>(2, HEARTBEAT_INTERVAL, queue, topic);

    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1)
        .isEqualTo(List.of(new StartWorkerTaskAction(), new StartWorkerTaskAction()));

    List<TaskAction> actions2 = controller.onWorkerStarted(1);
    assertThat(actions2).isEmpty();

    List<TaskAction> actions3 = controller.onWorkerStarted(2);
    assertThat(actions3).isEmpty();


    // When both workers fail exceptionally with different exceptions
    RuntimeException exception1 = new RuntimeException("Error 1");
    RuntimeException exception2 = new RuntimeException("Error 2");

    controller.onWorkerCompletedExceptionally(1, exception1);
    List<TaskAction> actions = controller.onWorkerCompletedExceptionally(2, exception2);

    // Then it should capture both exceptions
    assertThat(controller.state).isEqualTo(TaskState.FAILED);
    assertThat(controller.failureCause).isInstanceOf(ExecutionException.class);
    assertThat(controller.failureCause.getSuppressed()).hasSize(1);
    assertThat(actions).hasSize(1);
    assertThat(actions.get(0)).isInstanceOf(FailTaskAction.class);
  }

  @Test
  void givenCancelingState_whenCancelRequestedAgain_thenIgnoresExtraRequest() {
    // Given a controller in CANCELING state
    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1).isEqualTo(List.of(new StartWorkerTaskAction()));

    List<TaskAction> actions2 = controller.onWorkerStarted(1);
    assertThat(actions2).isEmpty();

    // When cancellation is requested
    List<TaskAction> actions3 = controller.onCancelRequested();
    assertThat(actions3).isEqualTo(List.of(new StopWorkerTaskAction()));

    // When cancel is requested again
    List<TaskAction> actions = controller.onCancelRequested();

    // Then it should ignore the duplicate request
    assertThat(actions).isEmpty();
  }

  @Test
  void givenRunningState_whenWorkerStopsUnexpectedly_thenStartsReplacementWorker() {
    // Given a controller in RUNNING state
    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1).isEqualTo(List.of(new StartWorkerTaskAction()));

    List<TaskAction> actions2 = controller.onWorkerStarted(1);
    assertThat(actions2).isEmpty();

    // When a worker stops unexpectedly
    List<TaskAction> actions = controller.onWorkerStopped(1);

    // Then it should start a replacement worker
    assertThat(actions).hasSize(1);
    assertThat(actions.get(0)).isInstanceOf(StartWorkerTaskAction.class);
  }

  @Test
  void givenNoWorkersRunning_whenCancelRequested_thenTransitionsDirectlyToCanceled() {
    // Given a controller with no workers running
    List<TaskAction> actions1 = controller.onTaskStart();
    assertThat(actions1).isEqualTo(List.of(new StartWorkerTaskAction()));

    // When cancellation is requested with no workers running
    List<TaskAction> actions2 = controller.onCancelRequested();

    // We should NOT transition directly to CANCELED, since we have pending starts
    List<TaskAction> actions3 = controller.onWorkerStarted(1);
    assertThat(actions3).isEqualTo(List.of(new StopWorkerTaskAction()));

    // Then it should transition to CANCELED
    List<TaskAction> actions4 = controller.onWorkerStopped(1);
    assertThat(controller.state).isEqualTo(TaskState.CANCELED);
    assertThat(actions4).isEqualTo(List.of(new CancelTaskAction()));
  }
}
