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
package io.aleph0.yap.core.pipeline;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.Measureable;
import io.aleph0.yap.core.Pipeline;
import io.aleph0.yap.core.PipelineCanceledException;
import io.aleph0.yap.core.PipelineErrorException;
import io.aleph0.yap.core.PipelineExecutionException;
import io.aleph0.yap.core.build.PipelineBuilder;
import io.aleph0.yap.core.pipeline.action.CancelPipelineAction;
import io.aleph0.yap.core.pipeline.action.CancelTaskPipelineAction;
import io.aleph0.yap.core.pipeline.action.FailPipelineAction;
import io.aleph0.yap.core.pipeline.action.PipelineAction;
import io.aleph0.yap.core.pipeline.action.StartTaskPipelineAction;
import io.aleph0.yap.core.pipeline.action.SucceedPipelineAction;
import io.aleph0.yap.core.task.TaskManager;

/**
 * PipelineManager is a class that manages the lifecycle of a {@link Pipeline}, including ask
 * lifecycle and lifecycle listeners. It is designed to be used in a multi-threaded environment and
 * provides methods for managing tasks.
 * 
 * <p>
 * The PipelineManager delegates decision making to a {@link PipelineController} instance, which is
 * responsible for deciding what actions to take based on the current state of the pipeline and
 * workers, and the PipelineManager is responsible for executing those actions.
 * 
 * <p>
 * Internally, the PipelineManager models the task state as a finite state machine with the
 * following states:
 * 
 * <pre>
 *                                  
 *     READY ─► RUNNING ─► COMPLETED
 *               │  │               
 *               │  └────► CANCELLED
 *               │                  
 *               └───────► FAILED
 * 
 * </pre>
 * 
 * <p>
 * To ensure correctness, it enforces that the pipeline can only perform the above transitions
 * between states. (So, for example, the PipelineManager will fail if the controller attempts to
 * transition from {@code COMPLETED} to {@code RUNNING} or from {@code FAILED} to
 * {@code CANCELLED}.) Within the boundaries of these constraints, the PipelineController is free to
 * decide what actions to take.
 * 
 */
public class PipelineManager implements Measureable<PipelineManager.Metrics>, Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineManager.class);

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

  /**
   * Overriden to provide a thread name for easier debugging.
   * 
   * @see PipelineBuilder#defaultVirtualThreadExecutorService()
   * @see PipelineBuilder#defaultPlatformThreadExecutorService()
   */
  private class MyPipelineTaskRunner implements PipelineTaskRunner {
    private final TaskManager body;

    public MyPipelineTaskRunner(TaskManager body) {
      this.body = requireNonNull(body);
    }

    @Override
    public int getPipelineId() {
      return PipelineManager.this.id;
    }

    @Override
    public String getTaskId() {
      return body.getId();
    }

    @Override
    public void run() {
      try {
        LOGGER.atDebug().addKeyValue("pipeline", id).addKeyValue("task", body.getId())
            .log("Pipeline task started");
        body.run();
        LOGGER.atDebug().addKeyValue("pipeline", id).addKeyValue("task", body.getId())
            .log("Pipeline task completed");
      } catch (InterruptedException e) {
        LOGGER.atInfo().addKeyValue("pipeline", id).addKeyValue("task", body.getId())
            .log("Pipeline task canceled");
      } catch (Throwable t) {
        LOGGER.atError().addKeyValue("pipeline", id).addKeyValue("task", body.getId()).setCause(t)
            .log("Pipeline task failed");
      }
    }

    @Override
    public String toString() {
      return String.format("<<pipeline-%d-task-%s>>", getPipelineId(), getTaskId());
    }
  }

  static sealed interface PipelineEvent permits TaskStartedEvent, TaskCompletedEvent,
      TaskFailedEvent, TaskCancelledEvent, WorkerLifecycleEvent {
  }

  static record TaskStartedEvent(String id) implements PipelineEvent {
  }

  static record TaskCompletedEvent(String id) implements PipelineEvent {
  }

  static record TaskFailedEvent(String id, ExecutionException cause) implements PipelineEvent {
  }

  static record TaskCancelledEvent(String id) implements PipelineEvent {
  }

  private static record WorkerLifecycleEvent(String taskId, int workerId,
      Consumer<LifecycleListener> event) implements PipelineEvent {
  }

  public static enum PipelinePhase {
    READY, RUNNING, FINISHED;
  }

  public static enum PipelineState {
    READY(PipelinePhase.READY) {
      @Override
      public PipelineState to(PipelineState target) {
        if (target != RUNNING)
          throw new IllegalStateException("Invalid transition from READY to " + target);
        return target;
      }
    },
    RUNNING(PipelinePhase.RUNNING) {
      @Override
      public PipelineState to(PipelineState target) {
        if (target != COMPLETED && target != CANCELLED && target != FAILED)
          throw new IllegalStateException("Invalid transition from RUNNING to " + target);
        return target;
      }
    },
    COMPLETED(PipelinePhase.FINISHED) {
      @Override
      public PipelineState to(PipelineState target) {
        throw new IllegalStateException("Invalid transition from COMPLETED to " + target);
      }
    },
    CANCELLED(PipelinePhase.FINISHED) {
      @Override
      public PipelineState to(PipelineState target) {
        throw new IllegalStateException("Invalid transition from CANCELLED to " + target);
      }
    },
    FAILED(PipelinePhase.FINISHED) {
      @Override
      public PipelineState to(PipelineState target) {
        throw new IllegalStateException("Invalid transition from FAILED to " + target);
      }
    };

    private final PipelinePhase phase;

    private PipelineState(PipelinePhase phase) {
      this.phase = requireNonNull(phase);
    }

    public PipelinePhase getPhase() {
      return phase;
    }

    /**
     * Validates that the transition from this state to the target state is valid.
     * 
     * @param target the target state
     * @return the target state
     * @throws IllegalStateException if the transition is invalid
     */
    public abstract PipelineState to(PipelineState target);
  }

  /**
   * Relay lifecycle events from the task manager to the pipeline controller that would not be
   * visible otherwise.
   */
  private class WorkerLifecycleListener implements TaskManager.LifecycleListener {
    @Override
    public void onTaskStarted(String task) {
      events.offer(new TaskStartedEvent(task));
    }

    @Override
    public void onTaskWorkerStarted(String task, int worker) {
      events.offer(new WorkerLifecycleEvent(task, worker,
          listener -> listener.onPipelineTaskWorkerStarted(id, task, worker)));
    }

    @Override
    public void onTaskWorkerStopRequested(String task, int worker) {
      events.offer(new WorkerLifecycleEvent(task, worker,
          listener -> listener.onPipelineTaskWorkerStopRequested(id, task, worker)));
    }

    @Override
    public void onTaskWorkerStopped(String task, int worker) {
      events.offer(new WorkerLifecycleEvent(task, worker,
          listener -> listener.onPipelineTaskWorkerStopped(id, task, worker)));
    }

    @Override
    public void onTaskWorkerCompletedNormally(String task, int worker) {
      events.offer(new WorkerLifecycleEvent(task, worker,
          listener -> listener.onPipelineTaskWorkerCompletedNormally(id, task, worker)));
    }

    @Override
    public void onTaskWorkerCompletedExceptionally(String task, int worker, Throwable cause) {
      events.offer(new WorkerLifecycleEvent(task, worker, listener -> listener
          .onPipelineTaskWorkerCompletedExceptionally(id, task, worker, cause)));
    }

    @Override
    public void onTaskCancelRequested(String task, int worker) {
      events.offer(new WorkerLifecycleEvent(task, worker,
          listener -> listener.onPipelineTaskCancelRequested(id, task, worker)));
    }

    @Override
    public void onTaskCompleted(String task) {
      events.offer(new TaskCompletedEvent(task));
    }

    @Override
    public void onTaskCancelled(String task) {
      events.offer(new TaskCancelledEvent(task));
    }

    @Override
    public void onTaskFailed(String task, ExecutionException cause) {
      events.offer(new TaskFailedEvent(task, cause));
    }
  }

  private final int id;
  private final ExecutorService executor;
  private final PipelineController controller;
  private final List<TaskManager<?>> taskBodies;
  private final Map<String, Future<?>> runningTasks = new ConcurrentHashMap<>();
  private final BlockingQueue<PipelineEvent> events = new LinkedBlockingQueue<>();
  private final List<LifecycleListener> lifecycleListeners = new CopyOnWriteArrayList<>();
  private volatile PipelineState state = PipelineState.READY;
  private ExecutionException failureCause;

  public PipelineManager(int id, ExecutorService executor, PipelineController controller,
      List<TaskManager<?>> tasks) {
    this.id = id;
    this.executor = requireNonNull(executor);
    this.controller = requireNonNull(controller);
    this.taskBodies = unmodifiableList(tasks);

  }

  public int getId() {
    return id;
  }

  public void addLifecycleListener(LifecycleListener listener) {
    if (listener == null)
      throw new NullPointerException();
    lifecycleListeners.add(listener);
  }

  public void removeLifecycleListener(LifecycleListener listener) {
    lifecycleListeners.remove(listener);
  }

  /**
   * Runs the pipeline manager, managing the lifecycle of the pipeline and its tasks.
   * 
   * @throws PipelineExecutionException if the pipeline fails normally
   * @throws PipelineCanceledException if the pipeline is cancelled
   * @throws PipelineErrorException if the pipeline fails due to an internal error
   */
  public void run() {
    try {
      for (TaskManager taskBody : taskBodies)
        taskBody.addLifecycleListener(new WorkerLifecycleListener());

      state = state.to(PipelineState.RUNNING);

      LOGGER.atDebug().addKeyValue("pipeline", id).log("Pipeline manager started");

      List<PipelineAction> actions = controller.onPipelineStarted();

      notifyLifecycleListeners(listener -> listener.onPipelineStarted(id));

      eventLoop(actions);

      switch (state) {
        case COMPLETED:
          LOGGER.atDebug().addKeyValue("pipeline", id).log("Pipeline completed");
          controller.onPipelineCompleted();
          notifyLifecycleListeners(listener -> listener.onPipelineCompleted(id));
          break;
        case CANCELLED:
          LOGGER.atWarn().addKeyValue("pipeline", id)
              .log("Pipeline cancelled, but without cancel request");
          controller.onPipelineCancelled();
          notifyLifecycleListeners(listener -> listener.onPipelineCancelled(id));
          break;
        case FAILED:
          LOGGER.atDebug().addKeyValue("pipeline", id).setCause(failureCause)
              .log("Pipeline failed");
          controller.onPipelineFailed(failureCause);
          notifyLifecycleListeners(listener -> listener.onPipelineFailed(id, failureCause));
          break;
        default:
          throw new IllegalStateException("Pipeline manager in invalid state after run: " + state);
      }
    } catch (InterruptedException e) {
      LOGGER.atInfo().addKeyValue("pipeline", id)
          .log("Pipeline manager interrupted, treating as cancel request");
      try {
        if (state.getPhase() != PipelinePhase.FINISHED) {
          List<PipelineAction> actions = controller.onCancelRequested();
          notifyLifecycleListeners(listener -> listener.onPipelineCancelRequested(id));
          eventLoop(actions);
        }
        switch (state) {
          case COMPLETED:
            LOGGER.atWarn().addKeyValue("pipeline", id)
                .log("Pipeline completed, but after cancel request");
            controller.onPipelineCompleted();
            notifyLifecycleListeners(listener -> listener.onPipelineCompleted(id));
            break;
          case CANCELLED:
            LOGGER.atInfo().addKeyValue("pipeline", id).log("Pipeline cancelled");
            controller.onPipelineCancelled();
            notifyLifecycleListeners(listener -> listener.onPipelineCancelled(id));
            break;
          case FAILED:
            LOGGER.atWarn().addKeyValue("pipeline", id).setCause(failureCause)
                .log("Pipeline failed, but after cancel request");
            controller.onPipelineFailed(failureCause);
            notifyLifecycleListeners(listener -> listener.onPipelineFailed(id, failureCause));
            break;
          default:
            throw new PipelineErrorException(new IllegalStateException(
                "Pipeline manager in invalid state after interrupt: " + state));
        }
      } catch (InterruptedException e2) {
        // OK, we were interrupted again while trying to cancel. Just propagate.
        LOGGER.atWarn().addKeyValue("pipeline", id)
            .log("Pipeline manager interrupted again while cancelling; propagating...");
        throw new PipelineCanceledException();
      } catch (Exception e2) {
        // Something went wrong while trying to cancel. Wrap and propagate.
        LOGGER.atError().addKeyValue("pipeline", id).setCause(e2)
            .log("Pipeline manager failed while cancelling; propagating...");
        throw new PipelineErrorException(e2);
      }
      throw new PipelineCanceledException();
    } catch (Exception e) {
      LOGGER.atError().addKeyValue("pipeline", id).setCause(e).log(
          "Pipeline manager failed; hard failing pipeline, canceling all tasks, and propagating exception...");
      final PipelineErrorException problem = new PipelineErrorException(e);
      try {
        state = PipelineState.FAILED;
        for (Future<?> taskFuture : runningTasks.values())
          taskFuture.cancel(true);
        notifyLifecycleListeners(listener -> {
          listener.onPipelineFailed(id, e);
        });
      } catch (Exception e2) {
        LOGGER.atError().addKeyValue("pipeline", id).setCause(e2)
            .log("Lifecycle listener threw exception during pipeline failure notification");
        problem.addSuppressed(e2);
      }
      throw problem;
    }

    // We want to propagate the failure cause if the pipeline failed for use in Future.get()
    if (state == PipelineState.FAILED && failureCause != null) {
      LOGGER.atDebug().addKeyValue("pipeline", id).setCause(failureCause)
          .log("Pipeline failed with exception, propagating...");
      throw new PipelineExecutionException(failureCause);
    }
    if (state == PipelineState.CANCELLED) {
      LOGGER.atDebug().addKeyValue("pipeline", id).log("Pipeline cancelled, propagating...");
      throw new PipelineCanceledException();
    }
  }

  private void eventLoop(List<PipelineAction> initialActions) throws InterruptedException {
    for (PipelineAction action : initialActions)
      performPipelineAction(action);
    while (state == PipelineState.RUNNING) {
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        throw new InterruptedException();
      }
      final PipelineEvent event =
          events.poll(controller.getHeartbeatInterval().toMillis(), TimeUnit.MILLISECONDS);
      List<PipelineAction> actions = handleTaskEvent(event);
      for (PipelineAction action : actions)
        performPipelineAction(action);
    }
  }

  List<PipelineAction> handleTaskEvent(PipelineEvent event) {
    List<PipelineAction> result;
    switch (event) {
      case

          TaskStartedEvent(String taskId): {
        LOGGER.atDebug().addKeyValue("pipeline", id).addKeyValue("task", taskId)
            .log("Task started");
        result = controller.onTaskStarted(taskId);
        notifyLifecycleListeners(listener -> listener.onPipelineTaskStarted(id, taskId));
        break;
      }
      case

          TaskCompletedEvent(String taskId): {
        LOGGER.atDebug().addKeyValue("pipeline", id).addKeyValue("task", taskId)
            .log("Task completed");
        runningTasks.remove(taskId);
        result = controller.onTaskCompleted(taskId);
        notifyLifecycleListeners(listener -> listener.onPipelineTaskCompleted(id, taskId));
        break;
      }
      case

          TaskFailedEvent(String taskId, Throwable cause): {
        LOGGER.atError().addKeyValue("pipeline", id).addKeyValue("task", taskId).setCause(cause)
            .log("Task failed");
        runningTasks.remove(taskId);
        result = controller.onTaskFailed(taskId, cause);
        notifyLifecycleListeners(listener -> listener.onPipelineTaskFailed(id, taskId, cause));
        break;
      }
      case

          TaskCancelledEvent(String taskId): {
        LOGGER.atDebug().addKeyValue("pipeline", id).addKeyValue("task", taskId)
            .log("Task cancelled");
        runningTasks.remove(taskId);
        result = controller.onTaskCancelled(taskId);
        notifyLifecycleListeners(listener -> listener.onPipelineTaskCancelled(id, taskId));
        break;
      }
      case

          WorkerLifecycleEvent(String taskId, int workerId, Consumer<LifecycleListener> e): {
        LOGGER.atDebug().addKeyValue("pipeline", id).addKeyValue("task", taskId)
            .addKeyValue("worker", workerId).log("Worker lifecycle event");
        result = emptyList();
        notifyLifecycleListeners(e);
        break;
      }
      case null: {
        LOGGER.atDebug().addKeyValue("pipeline", id).log("Heartbeat");
        result = controller.onHeartbeat();
        break;
      }
    };
    return result;

  }

  void performPipelineAction(PipelineAction action) throws InterruptedException {
    switch (action) {
      case

          StartTaskPipelineAction(String taskId): {
        LOGGER.atDebug().addKeyValue("pipeline", id).addKeyValue("task", taskId)
            .log("Starting pipeline task");
        final TaskManager taskBody = taskBodies.stream().filter(task -> task.getId().equals(taskId))
            .findFirst().orElseThrow();
        final MyPipelineTaskRunner task = new MyPipelineTaskRunner(taskBody);
        final Future<?> taskFuture = executor.submit(task);
        runningTasks.put(taskId, taskFuture);
        break;
      }
      case

          CancelTaskPipelineAction(String taskId): {
        LOGGER.atDebug().addKeyValue("pipeline", id).addKeyValue("task", taskId)
            .log("Cancelling pipeline task");
        final Future<?> taskFuture = Optional.ofNullable(runningTasks.get(taskId)).orElseThrow();
        taskFuture.cancel(true);
        break;
      }
      case SucceedPipelineAction(): {
        LOGGER.atDebug().addKeyValue("pipeline", id).log("Succeeded pipeline");
        state = state.to(PipelineState.COMPLETED);
        break;
      }
      case CancelPipelineAction(): {
        LOGGER.atDebug().addKeyValue("pipeline", id).log("Cancelled pipeline");
        state = state.to(PipelineState.CANCELLED);
        break;
      }
      case

          FailPipelineAction(ExecutionException cause): {
        LOGGER.atDebug().addKeyValue("pipeline", id).setCause(cause).log("Failed pipeline");
        state = state.to(PipelineState.FAILED);
        failureCause = cause;
        break;
      }
      case null: {
        // No action, just continue.
        LOGGER.atDebug().addKeyValue("pipeline", id).log("Do nothing");
        break;
      }
    }

  }

  private void notifyLifecycleListeners(Consumer<LifecycleListener> event) {
    for (LifecycleListener listener : lifecycleListeners) {
      try {
        event.accept(listener);
      } catch (Exception e) {
        LOGGER.atError().setCause(e).log("Lifecycle listener threw exception");
      }
    }
  }

  @Override
  public Metrics checkMetrics() {
    final PipelineState state = this.state;
    final PipelinePhase phase = state.getPhase();

    final Map<String, TaskManager.Metrics<?>> taskMetrics = new HashMap<>();
    for (TaskManager task : taskBodies)
      taskMetrics.put(task.getId(), task.checkMetrics());

    return new Metrics(id, phase, state, unmodifiableMap(taskMetrics));
  }

  @Override
  public Metrics flushMetrics() {
    Metrics result = checkMetrics();
    for (TaskManager task : taskBodies)
      task.flushMetrics();
    return result;
  }

  /**
   * Overriden to provide a thread name for easier debugging.
   * 
   * @see PipelineBuilder#defaultVirtualThreadExecutorService()
   * @see PipelineBuilder#defaultPlatformThreadExecutorService()
   */
  @Override
  public String toString() {
    return String.format("<<pipeline-%d-manager>>", id);
  }
}
