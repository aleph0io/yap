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

import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.Measureable;
import io.aleph0.yap.core.task.action.CancelTaskAction;
import io.aleph0.yap.core.task.action.FailTaskAction;
import io.aleph0.yap.core.task.action.StartWorkerTaskAction;
import io.aleph0.yap.core.task.action.StopWorkerTaskAction;
import io.aleph0.yap.core.task.action.SucceedTaskAction;
import io.aleph0.yap.core.task.action.TaskAction;
import io.aleph0.yap.core.transport.Queue;
import io.aleph0.yap.core.transport.Topic;

/**
 * TaskManager manages the lifecycle of a logical pipeline task, including starting and stopping
 * workers, handling task actions, and notifying lifecycle listeners. It is designed to be used in a
 * multi-threaded environment and provides methods for managing worker threads and task actions.
 * 
 * <p>
 * The TaskManager delegates decision making to a {@link TaskController} instance, which is
 * responsible for deciding what actions to take based on the current state of the task and the
 * workers, and the TaskManager is responsible for executing those actions.
 * 
 * <p>
 * Internally, the TaskManager models the task state as a finite state machine with the following
 * states:
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
 * To ensure correctness, it enforces that the task can only perform the above transitions between
 * states. (So, for example, the TaskManager will fail if the controller attempts to transition from
 * {@code COMPLETED} to {@code RUNNING} or from {@code FAILED} to {@code CANCELLED}.) Within the
 * boundaries of these constraints, the TaskController is completely free to decide what actions to
 * take.
 * 
 */
public class TaskManager<WorkerMetricsT>
    implements Measureable<TaskManager.Metrics<WorkerMetricsT>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskManager.class);

  public static record Metrics<WorkerMetricsT>(
      /**
       * The task id.
       */
      String id,

      /**
       * The current phase of the task.
       */
      TaskPhase phase,

      /**
       * The current state of the task.
       */
      TaskState state,

      /**
       * The worker metrics
       */
      WorkerMetricsT worker,

      /**
       * The number of worker threads currently running.
       */
      long workers,

      /**
       * The number of worker threads that have been started.
       */
      long commencements,

      /**
       * The number of worker threads that have completed, either normally or exceptionally.
       */
      long completions,

      /**
       * The number of worker threads that have completed normally.
       */
      long normalCompletions,

      /**
       * The number of worker threads that have stopped due to a cancel request.
       */
      long stopCompletions,

      /**
       * The number of worker threads that have completed exceptionally.
       */
      long exceptionalCompletions,

      /**
       * The number of inputs that are pending to be consumed by the task.
       */
      long pending,

      /**
       * The number of inputs consumed by the task.
       */
      long consumed,

      /**
       * The number of times the task had o wait for an input.
       */
      long waits,

      /**
       * The number of outputs produced by the task.
       */
      long produced,

      /**
       * The number of times the task stalled while trying to produce an output.
       */
      long stalls) {
    public Metrics {
      requireNonNull(id);
      requireNonNull(phase);
      requireNonNull(state);
      requireNonNull(worker);
      if (workers < 0)
        throw new IllegalArgumentException("workers must be greater than or equal to 0");
      if (commencements < 0)
        throw new IllegalArgumentException("commencements must be greater than or equal to 0");
      if (completions < 0)
        throw new IllegalArgumentException("completions must be greater than or equal to 0");
      if (normalCompletions < 0)
        throw new IllegalArgumentException("normalCompletions must be greater than or equal to 0");
      if (stopCompletions < 0)
        throw new IllegalArgumentException("stopCompletions must be greater than or equal to 0");
      if (exceptionalCompletions < 0)
        throw new IllegalArgumentException(
            "exceptionalCompletions must be greater than or equal to 0");
      if (pending < 0)
        throw new IllegalArgumentException("pending must be greater than or equal to 0");
      if (consumed < 0)
        throw new IllegalArgumentException("consumed must be greater than or equal to 0");
      if (waits < 0)
        throw new IllegalArgumentException("waits must be greater than or equal to 0");
      if (produced < 0)
        throw new IllegalArgumentException("produced must be greater than or equal to 0");
      if (stalls < 0)
        throw new IllegalArgumentException("stalls must be greater than or equal to 0");
    }
  }

  public static interface LifecycleListener {
    default void onTaskStarted(String task) {}

    default void onTaskWorkerStarted(String task, int worker) {}

    default void onTaskWorkerStopRequested(String task, int worker) {}

    default void onTaskWorkerStopped(String task, int worker) {}

    default void onTaskWorkerCompletedNormally(String task, int worker) {}

    default void onTaskWorkerCompletedExceptionally(String task, int worker, Throwable cause) {}

    default void onTaskCancelRequested(String task, int worker) {}

    default void onTaskCompleted(String task) {}

    default void onTaskCancelled(String task) {}

    default void onTaskFailed(String task, ExecutionException cause) {}
  }

  @FunctionalInterface
  public static interface WorkerBody {
    public void run() throws Exception;
  }

  public static interface WorkerBodyFactory<MetricsT> {
    public WorkerBody newWorkerBody();

    public MetricsT checkMetrics();

    public MetricsT flushMetrics();
  }

  private class WorkerRunner implements Runnable {
    private final int id;
    private final WorkerBody body;

    public WorkerRunner(int id, WorkerBody body) {
      this.id = id;
      this.body = requireNonNull(body);
    }

    @Override
    public void run() {
      try {
        offer(new WorkerStartedEvent(id));
        LOGGER.atDebug().addKeyValue("id", id).log("Worker started");

        body.run();

        offer(new WorkerCompletedEvent(id));
        LOGGER.atDebug().addKeyValue("id", id).log("Worker completed normally");
      } catch (InterruptedException e) {
        offer(new WorkerStoppedEvent(id));
        LOGGER.atInfo().addKeyValue("id", id).log("Worker stopped");
      } catch (Throwable t) {
        offer(new WorkerFailedEvent(id, t));
        LOGGER.atError().addKeyValue("id", id).setCause(t).log("Worker completed exceptionally");
      }
    }

    private void offer(WorkerEvent event) {
      boolean success = events.offer(event);
      assert success;
    }
  }

  static sealed interface WorkerEvent
      permits WorkerStartedEvent, WorkerCompletedEvent, WorkerFailedEvent, WorkerStoppedEvent {
  }

  static record WorkerStartedEvent(int id) implements WorkerEvent {
  }

  static record WorkerCompletedEvent(int id) implements WorkerEvent {
  }

  static record WorkerFailedEvent(int id, Throwable cause) implements WorkerEvent {
  }

  static record WorkerStoppedEvent(int id) implements WorkerEvent {
  }

  public static enum TaskPhase {
    READY, RUNNING, FINISHED;
  }

  public static enum TaskState {
    READY(TaskPhase.READY) {
      @Override
      public TaskState to(TaskState target) {
        if (target == RUNNING)
          return target;
        throw new IllegalStateException("Invalid transition from READY to " + target);
      }
    },
    RUNNING(TaskPhase.RUNNING) {
      @Override
      public TaskState to(TaskState target) {
        if (target == SUCCEEDED || target == CANCELED || target == FAILED)
          return target;
        throw new IllegalStateException("Invalid transition from RUNNING to " + target);
      }
    },
    SUCCEEDED(TaskPhase.FINISHED) {
      @Override
      public TaskState to(TaskState target) {
        throw new IllegalStateException("Invalid transition from SUCCEEDED to " + target);
      }
    },
    CANCELED(TaskPhase.FINISHED) {
      @Override
      public TaskState to(TaskState target) {
        throw new IllegalStateException("Invalid transition from CANCELED to " + target);
      }
    },
    FAILED(TaskPhase.FINISHED) {
      @Override
      public TaskState to(TaskState target) {
        throw new IllegalStateException("Invalid transition from FAILED to " + target);
      }
    };

    private final TaskPhase phase;

    private TaskState(TaskPhase phase) {
      this.phase = requireNonNull(phase);
    }

    public TaskPhase getPhase() {
      return phase;
    }

    /**
     * Validate that the transition to the target state is valid.
     * 
     * @param target the target state
     * @return the target state
     * @throws IllegalStateException if the transition is invalid
     */
    public abstract TaskState to(TaskState target);
  }

  private final AtomicInteger sequence = new AtomicInteger(0);
  private final AtomicLong metricCommencements = new AtomicLong(0);
  private final AtomicLong metricCompletions = new AtomicLong(0);
  private final AtomicLong metricNormalCompletions = new AtomicLong(0);
  private final AtomicLong metricStopCompletions = new AtomicLong(0);
  private final AtomicLong metricExceptionalCompletions = new AtomicLong(0);
  private final Map<Integer, Future<?>> workers = new HashMap<>();
  private final BlockingQueue<WorkerEvent> events = new LinkedBlockingQueue<>();
  private final List<LifecycleListener> lifecycleListeners = new CopyOnWriteArrayList<>();
  private final String id;
  private final Set<String> subscribers;
  private final ExecutorService executor;
  private final TaskController controller;
  private final WorkerBodyFactory<WorkerMetricsT> workerBodyFactory;
  private final Queue<?> queue;
  private final Topic<?> topic;
  private volatile TaskState state = TaskState.READY;
  private ExecutionException failureCause = null;

  public TaskManager(String id, Set<String> subscribers, ExecutorService executor,
      TaskController controller, WorkerBodyFactory<WorkerMetricsT> workerBodyFactory,
      Queue<?> queue, Topic<?> topic) {
    this.id = requireNonNull(id);
    this.subscribers = unmodifiableSet(subscribers);
    this.executor = requireNonNull(executor);
    this.controller = requireNonNull(controller);
    this.workerBodyFactory = requireNonNull(workerBodyFactory);
    this.queue = queue;
    this.topic = topic;
  }

  public String getId() {
    return id;
  }

  public Set<String> getSubscribers() {
    return subscribers;
  }

  public void addLifecycleListener(LifecycleListener listener) {
    if (listener == null)
      throw new NullPointerException();
    lifecycleListeners.add(listener);
  }

  public void removeLifecycleListener(LifecycleListener listener) {
    lifecycleListeners.remove(listener);
  }

  public void run() throws Exception {
    try {
      state = state.to(TaskState.RUNNING);

      LOGGER.atDebug().addKeyValue("task", id).log("Task runner started");

      List<TaskAction> actions = controller.onTaskStart();

      notifyLifecycleListeners(l -> l.onTaskStarted(id));

      eventLoop(actions);

      // We are done, stop all workers.
      switch (state) {
        case TaskState.SUCCEEDED:
          LOGGER.atDebug().addKeyValue("task", id).log("Task completed");
          controller.onTaskSucceeded();
          notifyLifecycleListeners(listener -> listener.onTaskCompleted(id));
          break;
        case TaskState.CANCELED:
          LOGGER.atWarn().addKeyValue("task", id).log("Task cancelled, but without cancel request");
          controller.onTaskCancelled();
          notifyLifecycleListeners(listener -> listener.onTaskCancelled(id));
          break;
        case TaskState.FAILED:
          LOGGER.atDebug().addKeyValue("task", id).setCause(failureCause).log("Task failed");
          controller.onTaskFailed(failureCause);
          notifyLifecycleListeners(listener -> listener.onTaskFailed(id, failureCause));
          break;
        default:
          throw new IllegalStateException("Task manager in invalid state after run: " + state);
      }

      LOGGER.atDebug().addKeyValue("task", id).log("Task runner completed normally");
    } catch (InterruptedException e) {
      // Clear the interrupt flag so we can handle it.
      Thread.interrupted();

      LOGGER.atInfo().addKeyValue("pipeline", id)
          .log("Pipeline manager interrupted, treating as cancel request");

      if (state.getPhase() != TaskPhase.FINISHED) {
        List<TaskAction> actions = controller.onCancelRequested();
        eventLoop(actions);
      }

      switch (state) {
        case SUCCEEDED:
          LOGGER.atWarn().addKeyValue("task", id).log("Task succeeded, but after cancel request");
          controller.onTaskSucceeded();
          notifyLifecycleListeners(listener -> listener.onTaskCompleted(id));
          break;
        case CANCELED:
          LOGGER.atInfo().addKeyValue("task", id).log("Task cancelled");
          controller.onTaskCancelled();
          notifyLifecycleListeners(listener -> listener.onTaskCancelled(id));
          break;
        case FAILED:
          LOGGER.atWarn().addKeyValue("task", id).setCause(failureCause)
              .log("Task failed, but after cancel request");
          controller.onTaskFailed(failureCause);
          notifyLifecycleListeners(listener -> listener.onTaskFailed(id, failureCause));
          break;
        default:
          throw new IllegalStateException(
              "Task manager in invalid state after interrupt: " + state);
      }
    } catch (Exception e) {
      LOGGER.atError().addKeyValue("task", id).setCause(e).log(
          "Task manager failed; hard failing task, canceling all workers, and propagating exception...");
      state = TaskState.FAILED;
      for (Future<?> workerFuture : workers.values())
        workerFuture.cancel(true);
      final ExecutionException cause = new ExecutionException(e);
      notifyLifecycleListeners(listener -> listener.onTaskFailed(id, cause));
      throw e;
    } finally {
      if (topic != null)
        topic.close();
    }
  }

  private void eventLoop(List<TaskAction> initialActions) throws InterruptedException {
    for (TaskAction action : initialActions)
      performTaskAction(action);
    while (state == TaskState.RUNNING) {
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        throw new InterruptedException();
      }
      final WorkerEvent event =
          events.poll(controller.getHeartbeatInterval().toMillis(), TimeUnit.MILLISECONDS);
      List<TaskAction> actions = handleWorkerEvent(event);
      for (TaskAction action : actions)
        performTaskAction(action);
    }
  }

  List<TaskAction> handleWorkerEvent(WorkerEvent event) {
    List<TaskAction> result;
    switch (event) {
      case WorkerStartedEvent started: {
        LOGGER.atDebug().addKeyValue("id", started.id).log("Worker started");
        // Added to workers in the action handler to capture Future object.
        result = controller.onWorkerStarted(started.id);
        metricCommencements.incrementAndGet();
        notifyLifecycleListeners(l -> l.onTaskWorkerStarted(id, started.id));
        break;
      }
      case WorkerCompletedEvent completed: {
        LOGGER.atDebug().addKeyValue("id", completed.id).log("Worker completed normally");
        workers.remove(completed.id);
        result = controller.onWorkerCompletedNormally(completed.id);
        metricCompletions.incrementAndGet();
        metricNormalCompletions.incrementAndGet();
        notifyLifecycleListeners(l -> l.onTaskWorkerCompletedNormally(id, completed.id));
        break;
      }
      case WorkerFailedEvent failed: {
        LOGGER.atError().addKeyValue("id", failed.id).setCause(failed.cause)
            .log("Worker completed exceptionally");
        workers.remove(failed.id);
        result = controller.onWorkerCompletedExceptionally(failed.id, failed.cause);
        metricCompletions.incrementAndGet();
        metricExceptionalCompletions.incrementAndGet();
        notifyLifecycleListeners(
            l -> l.onTaskWorkerCompletedExceptionally(id, failed.id, failed.cause));
        break;
      }
      case WorkerStoppedEvent stopped: {
        LOGGER.atDebug().addKeyValue("id", stopped.id).log("Worker stopped");
        workers.remove(stopped.id);
        result = controller.onWorkerStopped(stopped.id);
        metricCompletions.incrementAndGet();
        metricStopCompletions.incrementAndGet();
        notifyLifecycleListeners(l -> l.onTaskWorkerStopped(id, stopped.id));
        break;
      }
      case null: {
        LOGGER.atDebug().log("Heartbeat");
        result = controller.onHeartbeat();
        break;
      }
    }
    return result;
  }

  void performTaskAction(TaskAction action) throws InterruptedException {
    switch (action) {
      case StartWorkerTaskAction(): {
        final StartedWorker<WorkerMetricsT> w = startWorker();
        workers.put(w.id, w.future);
        LOGGER.atDebug().addKeyValue("id", w.id).log("Started worker");
        break;
      }
      case StopWorkerTaskAction(): {
        final int id = stopAnyWorker();
        LOGGER.atDebug().addKeyValue("id", id).log("Stopped worker");
        break;
      }
      case SucceedTaskAction(): {
        state = state.to(TaskState.SUCCEEDED);
        LOGGER.atDebug().log("Succeeded task");
        break;
      }
      case CancelTaskAction(): {
        state = state.to(TaskState.CANCELED);
        LOGGER.atDebug().log("Canceled task");
        break;
      }
      case FailTaskAction(ExecutionException cause): {
        state = state.to(TaskState.FAILED);
        failureCause = cause;
        LOGGER.atDebug().setCause(cause).log("Failed task");
        break;
      }
      case null: {
        // No action, just continue.
        LOGGER.atDebug().log("No action");
        break;
      }
    }
  }


  private static record StartedWorker<MetricsT>(int id, TaskManager<MetricsT>.WorkerRunner worker,
      Future<?> future) {
  }

  protected StartedWorker<WorkerMetricsT> startWorker() throws RejectedExecutionException {
    final int id = sequence.getAndIncrement();
    final WorkerBody body = workerBodyFactory.newWorkerBody();
    final WorkerRunner worker = new WorkerRunner(id, body);
    final Future<?> future = executor.submit(worker, null);
    return new StartedWorker<>(id, worker, future);
  }

  protected int stopAnyWorker() {
    if (workers.isEmpty())
      throw new IllegalStateException("no workers");
    final Iterator<Map.Entry<Integer, Future<?>>> iterator = workers.entrySet().iterator();
    final Map.Entry<Integer, Future<?>> entry = iterator.next();
    final int workerId = entry.getKey();
    final Future<?> workerFuture = entry.getValue();
    iterator.remove();
    workerFuture.cancel(true);
    return workerId;
  }

  protected void stopAllWorkers() {
    for (Future<?> future : workers.values())
      future.cancel(true);
  }

  @Override
  public Metrics<WorkerMetricsT> checkMetrics() {
    long pending, consumed, waits;
    if (queue != null) {
      final Queue.Metrics m = queue.checkMetrics();
      pending = m.pending();
      consumed = m.consumed();
      waits = m.waits();
    } else {
      pending = consumed = waits = 0;
    }

    long produced, stalls;
    if (topic != null) {
      final Topic.Metrics m = topic.checkMetrics();
      produced = m.published();
      stalls = m.stalls();
    } else {
      produced = stalls = 0;
    }

    final WorkerMetricsT worker = workerBodyFactory.checkMetrics();

    final TaskState state = this.state;
    final TaskPhase phase = state.getPhase();
    final long workers = this.workers.size();
    final long commencements = this.metricCommencements.get();
    final long completions = this.metricCompletions.get();
    final long normalCompletions = this.metricNormalCompletions.get();
    final long stopCompletions = this.metricStopCompletions.get();
    final long exceptionalCompletions = this.metricExceptionalCompletions.get();

    return new Metrics<>(id, phase, state, worker, workers, commencements, completions,
        normalCompletions, stopCompletions, exceptionalCompletions, pending, consumed, waits,
        produced, stalls);
  }

  @Override
  public Metrics<WorkerMetricsT> flushMetrics() {
    final Metrics<WorkerMetricsT> result = checkMetrics();
    if (queue != null)
      queue.flushMetrics();
    if (topic != null)
      topic.flushMetrics();
    workerBodyFactory.flushMetrics();
    metricCommencements.set(0);
    metricCompletions.set(0);
    metricNormalCompletions.set(0);
    metricStopCompletions.set(0);
    metricExceptionalCompletions.set(0);
    return result;
  }

  private void notifyLifecycleListeners(Consumer<LifecycleListener> event) {
    for (LifecycleListener lifecycleListener : lifecycleListeners) {
      try {
        event.accept(lifecycleListener);
      } catch (Exception e) {
        LOGGER.atError().setCause(e).log("Error notifying lifecycle listener");
      }
    }
  }
}
