package io.aleph0.yap.core.task;

import static java.util.Objects.requireNonNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.ConsumerWorker;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.ProducerWorker;
import io.aleph0.yap.core.task.TaskManager.WorkerEvent;
import io.aleph0.yap.core.task.action.TaskAction;
import io.aleph0.yap.core.transport.Queue;
import io.aleph0.yap.core.transport.Topic;

public class DefaultTaskController<InputT, OutputT> implements TaskController {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTaskController.class);

  /**
   * The phase of the task. Tasks always move through the phases in the same order:
   * 
   * <ol>
   * <li>{@link #STARTING}</li>
   * <li>{@link #RUNNING}</li>
   * <li>{@link #FINISHING}</li>
   * <li>{@link #FINISHED}</li>
   * </ol>
   */
  public static enum TaskPhase {
    /**
     * The task is starting up. The controller is waiting for the task to start.
     */
    STARTING,

    /**
     * The task is running. The controller is waiting for the task to finish processing.
     */
    RUNNING,

    /**
     * The task is shutting down. The controller is waiting for the task to shut down.
     */
    FINISHING,

    /**
     * The task is done.
     */
    FINISHED;
  }

  /**
   * The internal state of a task. The task starts in the {@link TaskState#READY} state. The
   * controller then moves the task through states according to the various {@link WorkerEvent
   * events} that occur.
   */
  public static enum TaskState {
    /**
     * The initial state of the task. {@link #onTaskStart() On task start}, the controller
     * immediately moves to {@link #RUNNING}.
     */
    READY(TaskPhase.STARTING) {
      @Override
      public TaskState to(TaskState target) {
        if (target == RUNNING)
          return RUNNING;
        throw new IllegalStateException("cannot transition from READY to " + target);
      }
    },

    /**
     * The task is running. One or more worker has been started, no workers have completed normally,
     * no workers have completed exceptionally, and the user has not requested cancellation. The
     * controller stays in this state until one of the following events:
     * 
     * <ul>
     * <li>If {@link #onWorkerCompletedNormally() a worker completes normally}, then move to
     * {@link #COMPLETING}</li>
     * <li>If {@link #onWorkerCompletedExceptionally(int,Throwable) a worker completes
     * exceptionally}, then move to {@link #FAILING}.</li>
     * <li>If {@link #onCancelRequested() the user requests cancellation}, then move to
     * {@link #CANCELING}.</li>
     * </ul>
     */
    RUNNING(TaskPhase.RUNNING) {
      @Override
      public TaskState to(TaskState target) {
        if (target == COMPLETING || target == COMPLETED || target == CANCELING || target == FAILING
            || target == FAILED) {
          return target;
        }
        throw new IllegalStateException("cannot transition from RUNNING to " + target);
      }
    },

    /**
     * The task is done processing and is in the process of shutting down. The following conditions
     * must be true:
     * 
     * <ul>
     * <li>At least one worker has completed normally</li>
     * <li>At least one worker is still running</li>
     * <li>No workers have completed exceptionally</li>
     * <li>The user has not requested cancellation</li>
     * </ul>
     * 
     * <p>
     * The controller stays in this state until one of the following events:
     * 
     * <ul>
     * <li>If the last worker {@link #onWorkerCompletedNormally() completes normally}, then move to
     * {@link #COMPLETED}.</li>
     * <li>
     * <li>If a worker {@link #onWorkerCompletedExceptionally(int,Throwable) completes
     * exceptionally}, then move to {@link #FAILING}. If it is the last worker, then move to
     * {@link #FAILED} instead.</li>
     * <li>If {@link #onCancelRequested() the user requests cancellation}, then move to
     * {@link #CANCELING}.</li>
     * </ul>
     */
    COMPLETING(TaskPhase.FINISHING) {
      @Override
      public TaskState to(TaskState target) {
        if (target == COMPLETED || target == CANCELING || target == FAILING || target == FAILED)
          return target;
        throw new IllegalStateException("cannot transition from COMPLETING to " + target);
      }
    },

    /**
     * The user has {@link #onCancelRequested() requested cancellation} and the task is in the
     * process of shutting down. The following conditions must be true:
     * 
     * <ul>
     * <li>The user has requested cancellation</li>
     * <li>At least one worker is still running</li>
     * <li>No workers have completed exceptionally</li>
     * </ul>
     * 
     * <p>
     * The controller stays in this state until one of the following events:
     * 
     * <ul>
     * <li>If the last worker {@link #onWorkerCompletedNormally() completes normally} or
     * {@link #onWorkerStopped stops}, then move to {@link #CANCELED}.</li>
     * <li>If a worker {@link #onWorkerCompletedExceptionally(int,Throwable) completes
     * exceptionally}, then move to {@link #FAILING}. If it is the last worker, then move to
     * {@link #FAILED} instead.</li>
     * </ul>
     */
    CANCELING(TaskPhase.FINISHING) {
      @Override
      public TaskState to(TaskState target) {
        if (target == CANCELED || target == FAILING || target == FAILED)
          return target;
        throw new IllegalStateException("cannot transition from CANCELING to " + target);
      }
    },

    /**
     * One or more workers have {@link #onWorkerCompletedExceptionally(int,Throwable) completed
     * exceptionally} and the task is in the process of shutting down. The following conditions must
     * be true:
     * 
     * <ul>
     * <li>At least one worker has completed exceptionally</li>
     * <li>At least one worker is still running</li>
     * </ul>
     * 
     * <p>
     * The controller stays in this state until one of the following events:
     * 
     * <ul>
     * <li>The last worker {@link #onWorkerCompletedNormally() normally},
     * {@link #onWorkerCompletedExceptionally(int,Throwable) exceptionally}, or
     * {@link #onWorkerStopped(int) stops}, then move to {@link #FAILED}.</li>
     * </ul>
     */
    FAILING(TaskPhase.FINISHING) {
      @Override
      public TaskState to(TaskState target) {
        if (target == FAILED)
          return target;
        throw new IllegalStateException("cannot transition from FAILING to " + target);
      }
    },

    /**
     * The task is done processing and has shut down. The user did not request cancellation, and all
     * workers have completed normally.
     */
    COMPLETED(TaskPhase.FINISHED) {
      @Override
      public TaskState to(TaskState target) {
        throw new IllegalStateException("cannot transition from COMPLETED to " + target);
      }
    },

    /**
     * The task is done processing and has shut down. The user requested cancellation, and all
     * workers either completed normally or stopped.
     */
    CANCELED(TaskPhase.FINISHED) {
      @Override
      public TaskState to(TaskState target) {
        throw new IllegalStateException("cannot transition from CANCELED to " + target);
      }
    },

    /**
     * The task is done processing and has shut down. At least one worker completed exceptionally.
     */
    FAILED(TaskPhase.FINISHED) {
      @Override
      public TaskState to(TaskState target) {
        throw new IllegalStateException("cannot transition from FAILED to " + target);
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
     * Validate the transition from this state to the target state. The controller will call this
     * method to validate the transition before moving to the target state. If the transition is
     * valid, the method will return the target state. If the transition is invalid, the method will
     * throw an {@link IllegalStateException}.
     * 
     * @param target the target state
     * @return the target state, if the transition is valid
     * @throws IllegalStateException if the transition is invalid
     */
    public abstract TaskState to(TaskState target);
  }

  /**
   * The desired number of parallel workers for this task. The controller will attempt to keep this
   * many workers running at all times, unless the task is shutting down.
   */
  private final int desiredConcurrency;

  /**
   * The longest time the {@link TaskManager task manager} should wait for an event before making a
   * heartbeat call to this controller. The heartbeat is design to allow controllers to perform
   * performance tuning, like starting new workers in response to load or topic stalls or queue
   * waits. If the controller does not want to do any performance tuning, it can set a long
   * heartbeat interval and ignore heartbeat calls.
   */
  private final Duration heartbeatInterval;

  /**
   * The {@link Queue queue} from which this task will read input. The controller is free to use
   * metrics from this queue to make task management decisions. The queue is present for tasks which
   * take input from upstream tasks (i.e., {@link ProcessorWorker processors} and
   * {@link ConsumerWorker consumers}). It is {@code null} for tasks which do not take input from an
   * upstream task (i.e., {@link ProducerWorker producers}).
   */
  private final Queue<InputT> queue;

  /**
   * The {@link Topic topic} to which this task will write output. The controller is free to use
   * metrics from this topic to make task management decisions. The topic is present for tasks which
   * produce output to downstream tasks (i.e., {@link ProducerWorker producer} and
   * {@link ProcessorWorker processors}). It is {@code null} for tasks which do not produce output
   * to a downstream task (i.e., {@link ConsumerWorker consumers}).
   */
  private final Topic<OutputT> topic;

  /**
   * One or more workers have completed exceptionally. The controller will fail the task and use
   * this exception as the cause of the failure.
   */
  ExecutionException failureCause;

  /**
   * The current state of the task. The controller will move the task through states according to
   * the state machine defined in {@link TaskState}.
   */
  TaskState state = TaskState.READY;

  /**
   * The number of workers that are currently running.
   */
  int workers = 0;

  /**
   * The number of workers that are starting, but haven't started yet.
   */
  int starting = 0;

  public DefaultTaskController(int desiredConcurrency, Duration heartbeatInterval,
      Queue<InputT> queue, Topic<OutputT> topic) {
    if (desiredConcurrency < 1)
      throw new IllegalArgumentException("parallelism must be at least 1");
    if (heartbeatInterval == null)
      throw new NullPointerException();
    if (!heartbeatInterval.isPositive())
      throw new IllegalArgumentException("heartbeatInterval must be positive");
    this.desiredConcurrency = desiredConcurrency;
    this.heartbeatInterval = heartbeatInterval;
    this.queue = queue;
    this.topic = topic;
  }

  @Override
  public List<TaskAction> onTaskStart() {
    final List<TaskAction> actions = new ArrayList<>();
    switch (state) {
      case READY:
        state = state.to(TaskState.RUNNING);
        for (int i = 1; i <= desiredConcurrency; i++) {
          actions.add(TaskAction.newStartWorkerTaskAction());
          starting = starting + 1;
        }
        break;
      default:
        throw new IllegalStateException("task in state " + state);
    }
    return actions;
  }

  @Override
  public List<TaskAction> onWorkerStarted(int id) {
    starting = starting - 1;
    workers = workers + 1;

    final List<TaskAction> actions = new ArrayList<>();
    switch (state) {
      case RUNNING:
        // Great news!
        break;
      case COMPLETING:
        // The new task should stop on its own.
        break;
      case CANCELING:
      case FAILING:
        // We're trying to shut down, cancel this guy.
        actions.add(TaskAction.newStopWorkerTaskAction());
        break;
      default:
        throw new IllegalStateException("task in state " + state);
    }

    return actions;
  }

  @Override
  public List<TaskAction> onWorkerCompletedNormally(int id) {
    workers = workers - 1;

    final List<TaskAction> actions = new ArrayList<>();
    switch (state) {
      case RUNNING:
        if (workers == 0) {
          state = state.to(TaskState.COMPLETED);
          actions.add(TaskAction.newSucceedTask());
        } else
          state = state.to(TaskState.COMPLETING);
        break;
      case COMPLETING:
        if (workers == 0) {
          state = state.to(TaskState.COMPLETED);
          actions.add(TaskAction.newSucceedTask());
        }
        break;
      case CANCELING:
        if (workers == 0) {
          state = state.to(TaskState.CANCELED);
          actions.add(TaskAction.newCancelTask());
        }
        break;
      case FAILING:
        if (workers == 0) {
          state = state.to(TaskState.FAILED);
          actions.add(TaskAction.newFailTask(failureCause));
        }
        break;
      default:
        throw new IllegalStateException("task in state " + state);
    }

    return actions;
  }

  @Override
  public List<TaskAction> onWorkerStopped(int id) {
    workers = workers - 1;

    final List<TaskAction> actions = new ArrayList<>();
    switch (state) {
      case RUNNING:
        // This is very strange. The worker stopped at our request, but we did not request it.
        // We're going to start a new worker to replace the lost one. We'll react differently if
        // and when we get our own cancel request. Note that this DOES NOT trigger us to move to
        // COMPLETING. We only move to COMPLETING when we get a normal completion.
        LOGGER.atWarn().addKeyValue("id", id).addKeyValue("state", state)
            .log("Worker stopped unexpectedly in RUNNING state.");
        starting = starting + 1;
        actions.add(TaskAction.newStartWorkerTaskAction());
        break;
      case COMPLETING:
        // I'd expect more normal completions, not stops, but fine...
        LOGGER.atWarn().addKeyValue("id", id).addKeyValue("state", state)
            .log("Worker stopped unexpectedly in COMPLETING state.");
        if (workers == 0) {
          state = state.to(TaskState.COMPLETED);
          actions.add(TaskAction.newSucceedTask());
        }
        break;
      case CANCELING:
        if (workers == 0) {
          state = state.to(TaskState.CANCELED);
          actions.add(TaskAction.newCancelTask());
        }
        break;
      case FAILING:
        if (workers == 0) {
          state = state.to(TaskState.FAILED);
          actions.add(TaskAction.newFailTask(failureCause));
        }
        break;
      default:
        throw new IllegalStateException("task in state " + state);
    }

    return actions;
  }

  @Override
  public List<TaskAction> onWorkerCompletedExceptionally(int id, Throwable error) {
    workers = workers - 1;

    // Update our failure cause. Try to keep all the problems.
    while (error instanceof ExecutionException)
      error = error.getCause();
    if (failureCause == null)
      failureCause = new ExecutionException("Error in task " + id, error);
    else
      failureCause.addSuppressed(error);

    final List<TaskAction> actions = new ArrayList<>();
    switch (state) {
      case RUNNING:
      case COMPLETING:
      case CANCELING:
        // Welp, this is bad. Fail the task.
        state = state.to(TaskState.FAILING);
        if (workers != 0) {
          for (int i = 1; i <= workers; i++)
            actions.add(TaskAction.newStopWorkerTaskAction());
        }
        // Fall through...
      case FAILING:
        if (workers == 0) {
          state = state.to(TaskState.FAILED);
          actions.add(TaskAction.newFailTask(failureCause));
        }
        break;
      default:
        throw new IllegalStateException("task in state " + state);
    }

    return actions;
  }

  @Override
  public List<TaskAction> onCancelRequested() {
    final List<TaskAction> actions = new ArrayList<>();
    switch (state) {
      case RUNNING:
      case COMPLETING:
        // Uh, sure, you're the boss. We'll stop the workers.
        state = state.to(TaskState.CANCELING);
        if (workers == 0 && starting == 0) {
          state = state.to(TaskState.CANCELED);
          actions.add(TaskAction.newCancelTask());
        } else {
          for (int i = 0; i < workers; i++)
            actions.add(TaskAction.newStopWorkerTaskAction());
        }
        break;
      case FAILING:
        // We don't override a failing state with a cancel. We just let the failure run its course.
        LOGGER.atWarn().addKeyValue("state", state)
            .log("Cancel requested in FAILING state. Ignoring.");
        break;
      case CANCELING:
        // We're already canceling. Nothing to do.
        LOGGER.atWarn().addKeyValue("state", state)
            .log("Cancel requested in CANCELING state. Ignoring.");
        break;
      default:
        throw new IllegalStateException("task in state " + state);
    }

    return actions;
  }

  @Override
  public List<TaskAction> onHeartbeat() {
    if (state != TaskState.RUNNING) {
      // This is odd, but not necessarily bad. It should represent a long wait. If we're shutting
      // down, then we should not be having long waits.
      LOGGER.atWarn().addKeyValue("state", state).log("Heartbeat received in non-running state");
    }

    // If the user wanted to do any performance tuning, like starting new workers in response to
    // load or topic stalls or queue waits, this would be a great place to do it. The framework
    // was designed with that use case in mind, but absent a real-world example, we didn't actually
    // implement it, so this is all just theoretical for now.


    return List.of();
  }

  @Override
  public void onTaskSucceeded() {}

  @Override
  public void onTaskCancelled() {}

  @Override
  public void onTaskFailed(ExecutionException e) {}

  @Override
  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }
}
