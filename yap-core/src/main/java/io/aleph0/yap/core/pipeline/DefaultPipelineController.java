package io.aleph0.yap.core.pipeline;

import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import io.aleph0.yap.core.build.PipelineControllerBuilder;
import io.aleph0.yap.core.pipeline.action.PipelineAction;
import io.aleph0.yap.core.task.DefaultTaskController.TaskState;

public class DefaultPipelineController implements PipelineController {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(DefaultPipelineController.class);

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements PipelineControllerBuilder {
    private Duration heartbeatInterval = Duration.ofMinutes(1L);

    public Builder setHeartbeatInterval(Duration heartbeatInterval) {
      if (heartbeatInterval == null)
        throw new NullPointerException();
      if (heartbeatInterval.isNegative() || heartbeatInterval.isZero())
        throw new IllegalArgumentException("heartbeatInterval must be positive");
      this.heartbeatInterval = heartbeatInterval;
      return this;
    }

    @Override
    public DefaultPipelineController build(Map<String, Set<String>> graph) {
      return new DefaultPipelineController(graph, heartbeatInterval);
    }
  }

  /**
   * The internal state of a pipeline. The pipeline starts in the {@link TaskState#READY} state. The
   * controller then moves the pipeline through states according to the various events that occur,
   * where events are modeled as methods in this class.
   */
  public static enum PipelineState {
    /**
     * The initial state of the pipeline. {@link #onPipelineStart() On task start}, the controller
     * immediately moves to {@link #RUNNING}.
     */
    READY {
      @Override
      public PipelineState to(PipelineState target) {
        if (target == RUNNING)
          return target;
        throw new IllegalStateException("cannot transition from READY to " + target);
      }
    },

    /**
     * The pipeline is running. The following conditions must be true:
     * 
     * <ul>
     * <li>One or more tasks have been started</li>
     * <li>No tasks have failed</li>
     * <li>The user has not requested cancellation</li>
     * </ul>
     * 
     * <p>
     * The controller stays in this state until one of the following events:
     * 
     * <ul>
     * <li>If all tasks are started and subsequently {@link #onTaskCompleted(String) complete}, then
     * move to {@link #COMPLETED}</li>
     * <li>If {@link #onCancelRequested() the user requests cancellation}, then move to
     * {@link #CANCELING}.</li>
     * <li>If a task {@link #onTaskFailed(int,Throwable) fails}, then move to {@link #FAILING}. If
     * it is the last task, then move to {@link #FAILED} instead.</li>
     * </ul>
     */
    RUNNING {
      @Override
      public PipelineState to(PipelineState target) {
        if (target == COMPLETED || target == CANCELING || target == FAILING || target == FAILED)
          return target;
        throw new IllegalStateException("cannot transition from RUNNING to " + target);
      }
    },

    /**
     * The user has {@link #onCancelRequested() requested cancellation} and the pipeline is in the
     * process of shutting down. The following conditions must be true:
     * 
     * <ul>
     * <li>The user has requested cancellation</li>
     * <li>At least one task is still running</li>
     * <li>No tasks have failed</li>
     * </ul>
     * 
     * <p>
     * The controller stays in this state until one of the following events:
     * 
     * <ul>
     * <li>If the last worker {@link #onTaskCompleted(String) completes} or
     * {@link #onTaskCanceled(String) cancels}, then move to {@link #CANCELED}.</li>
     * <li>If a task {@link #onTaskFailed(String,Throwable) fails}, then move to {@link #FAILING}.
     * If it is the last task, then move to {@link #FAILED} instead.</li>
     * </ul>
     */
    CANCELING {
      @Override
      public PipelineState to(PipelineState target) {
        if (target == CANCELED || target == FAILING || target == FAILED)
          return target;
        throw new IllegalStateException("cannot transition from CANCELING to " + target);
      }
    },

    /**
     * One or more tasks have {@link #onTaskFailed(String,Throwable) failed} and the pipeline is in
     * the process of shutting down. The following conditions must be true:
     * 
     * <ul>
     * <li>At least one task has failed</li>
     * <li>At least one task is still running</li>
     * </ul>
     * 
     * <p>
     * The controller stays in this state until one of the following events:
     * 
     * <ul>
     * <li>The last task {@link #onTaskCompleted(String) completes},
     * {@link #onTaskFailed(String,Throwable) fails}, or {@link #onTaskCanceled(String) cancels},
     * then move to {@link #FAILED}.</li>
     * </ul>
     */
    FAILING {
      @Override
      public PipelineState to(PipelineState target) {
        if (target == FAILED)
          return target;
        throw new IllegalStateException("cannot transition from FAILING to " + target);
      }
    },

    /**
     * The pipeline is done processing and has shut down. The user did not request cancellation, and
     * all tasks completed normally. This is a terminal state.
     */
    COMPLETED {
      @Override
      public PipelineState to(PipelineState target) {
        throw new IllegalStateException("cannot transition from COMPLETED to " + target);
      }
    },

    /**
     * The pipeline is done processing and has shut down. The user requested cancellation, and all
     * tasks either completed normally or canceled. This is a terminal state.
     */
    CANCELED {
      @Override
      public PipelineState to(PipelineState target) {
        throw new IllegalStateException("cannot transition from CANCELED to " + target);
      }
    },

    /**
     * The pipeline is done processing and has shut down. At least one task failed. This is a
     * terminal state.
     */
    FAILED {
      @Override
      public PipelineState to(PipelineState target) {
        throw new IllegalStateException("cannot transition from FAILED to " + target);
      }
    };

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
    public abstract PipelineState to(PipelineState target);
  }

  private final Set<String> running = new LinkedHashSet<>();

  private final Set<String> starting = new LinkedHashSet<>();

  PipelineState state = PipelineState.READY;

  /**
   * A representation of the pipeline graph. Keys are task IDs, and values are the IDs of tasks that
   * subscribe to the task, i.e., treat it as a source of data.
   */
  private final Map<String, Set<String>> subscribers;

  /**
   * A representation of the pipeline graph. Keys are task IDs, and values are the IDs of tasks that
   * publish to the task, i.e., treat it as a sink of data.
   */
  private final Map<String, Set<String>> publishers;

  private final Duration heartbeatInterval;

  private ExecutionException failureReason;

  /**
   * Create a new pipeline controller.
   * 
   * @param graph The pipeline graph, where keys are task IDs and values are the IDs of tasks that
   *        subscribe to the task, i.e., treat it as a source of data.
   */
  public DefaultPipelineController(Map<String, Set<String>> graph, Duration heartbeatInterval) {
    this.subscribers = graph.entrySet().stream()
        .collect(toMap(Map.Entry::getKey, e -> unmodifiableSet(e.getValue())));
    this.publishers = graph.entrySet().stream()
        .flatMap(e -> e.getValue().stream().map(subscriber -> Map.entry(subscriber, e.getKey())))
        .collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toSet())));
    this.heartbeatInterval = heartbeatInterval;
  }

  @Override
  public List<PipelineAction> onPipelineStarted() {
    final List<PipelineAction> result = new ArrayList<>();

    switch (state) {
      case READY:
        state = state.to(PipelineState.RUNNING);

        for (Map.Entry<String, Set<String>> entry : subscribers.entrySet()) {
          final String taskId = entry.getKey();
          final Set<String> taskSubscribers = entry.getValue();
          if (running.containsAll(taskSubscribers) && !running.contains(taskId)
              && !starting.contains(taskId)) {
            // Track which tasks we kicked off so we don't start them multiple times in fan-in
            // topologies.
            result.add(PipelineAction.startTask(taskId));
            starting.add(taskId);
          }
        }

        break;
      default:
        throw new IllegalStateException("pipeline in state " + state);
    }


    return result;
  }

  @Override
  public List<PipelineAction> onTaskStarted(String id) {
    boolean removed = starting.remove(id);
    if (!removed) {
      // This is... odd. We started a task, but we didn't request it to start.
      LOGGER.atWarn().addKeyValue("task", id).log("Task started, but there was no start request");
    }

    running.add(id);

    final List<PipelineAction> result = new ArrayList<>();
    for (Map.Entry<String, Set<String>> entry : subscribers.entrySet()) {
      final String taskId = entry.getKey();
      final Set<String> taskSubscribers = entry.getValue();
      if (running.containsAll(taskSubscribers) && !running.contains(taskId)
          && !starting.contains(taskId)) {
        // We can only start one at a time. If we try to start multiple tasks at once, then we
        // get confused and start tasks multiple times in fan-in topologies.
        result.add(PipelineAction.startTask(taskId));
        starting.add(taskId);
      }
    }

    return result;
  }

  @Override
  public List<PipelineAction> onTaskCompleted(String id) {
    running.remove(id);

    List<PipelineAction> result = new ArrayList<>();
    switch (state) {
      case RUNNING:
        // TODO verify that all producing tasks are done
        for (Map.Entry<String, Set<String>> entry : subscribers.entrySet()) {
          final String taskId = entry.getKey();
          final Set<String> taskSubscribers = entry.getValue();
          if (running.contains(taskId) && !running.containsAll(taskSubscribers)) {
            // This is bad. A task has completed, but not all of its subscribers are completed yet.
            // So the task ended early. That's a failure. We can safely transition to FAILURE and
            // not check about FAILED because we know at least one task is still running.
            state = state.to(PipelineState.FAILING);
            failureReason = new ExecutionException(new IllegalStateException(
                "Task " + id + " completed, but not all subscribers are done"));
            break;
          }
        }
        if (state == PipelineState.FAILING) {
          for (String taskId : running)
            result.add(PipelineAction.cancelTask(taskId));
        } else if (running.isEmpty()) {
          state = state.to(PipelineState.COMPLETED);
          result.add(PipelineAction.succeed());
        }
        break;
      case CANCELING:
        if (running.isEmpty()) {
          state = state.to(PipelineState.CANCELED);
          return List.of(PipelineAction.cancel());
        }
        break;
      case FAILING:
        if (running.isEmpty()) {
          state = state.to(PipelineState.FAILED);
          return List.of(PipelineAction.fail(failureReason));
        }
        break;
      default:
        throw new IllegalStateException("pipeline in state " + state);
    }

    return result;
  }

  @Override
  public List<PipelineAction> onTaskCancelled(String id) {
    running.remove(id);

    final List<PipelineAction> result = new ArrayList<>();
    switch (state) {
      case RUNNING:
        // A task canceled, but we didn't cancel it. Someone's messing with our stuff. This is bad.
        // Fail the pipeline.
        LOGGER.atWarn().addKeyValue("task", id)
            .log("Task canceled, but there was no cancellation request. Failing pipeline...");
        state = state.to(PipelineState.FAILING);
        failureReason = new ExecutionException(new IllegalStateException(
            "Task " + id + " canceled, but no cancellation was requested"));
        if (running.isEmpty()) {
          state = state.to(PipelineState.FAILED);
          result.add(PipelineAction.fail(failureReason));
        } else {
          for (String taskId : running)
            result.add(PipelineAction.cancelTask(taskId));
        }
        break;
      case CANCELING:
        if (running.isEmpty()) {
          state = state.to(PipelineState.CANCELED);
          return List.of(PipelineAction.cancel());
        }
        break;
      case FAILING:
        if (running.isEmpty()) {
          state = state.to(PipelineState.FAILED);
          return List.of(PipelineAction.fail(failureReason));
        }
        break;
      default:
        throw new IllegalStateException("pipeline in state " + state);
    }

    return result;
  }

  @Override
  public List<PipelineAction> onTaskFailed(String id, Throwable error) {
    running.remove(id);

    while (error instanceof ExecutionException)
      error = error.getCause();
    if (failureReason == null)
      failureReason = new ExecutionException(error);
    else
      failureReason.addSuppressed(error);

    final List<PipelineAction> result = new ArrayList<>();
    switch (state) {
      case RUNNING:
      case CANCELING:
        // Welp, this is bad. Fail the pipeline.
        state = state.to(PipelineState.FAILING);
        for (String taskId : running)
          result.add(PipelineAction.cancelTask(taskId));
        // Fall through...
      case FAILING:
        if (running.isEmpty()) {
          state = state.to(PipelineState.FAILED);
          return List.of(PipelineAction.fail(failureReason));
        }
        break;
      default:
        throw new IllegalStateException("pipeline in state " + state);
    }

    return result;
  }

  @Override
  public List<PipelineAction> onHeartbeat() {
    // NOP
    return List.of();
  }

  @Override
  public List<PipelineAction> onCancelRequested() {
    final List<PipelineAction> result = new ArrayList<>();

    switch (state) {
      case RUNNING:
        state = state.to(PipelineState.CANCELING);
        for (String taskId : running)
          result.add(PipelineAction.cancelTask(taskId));
        break;
      case CANCELING:
        // Yep. You really want to cancel. I get it.
        break;
      case FAILING:
        // We're already failing. No need to cancel.
        break;
      default:
        throw new IllegalStateException("pipeline in state " + state);
    }

    return result;
  }

  @Override
  public void onPipelineCompleted() {}

  @Override
  public void onPipelineFailed(ExecutionException error) {}

  @Override
  public void onPipelineCancelled() {}

  @Override
  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }
}
