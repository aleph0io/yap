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
import io.aleph0.yap.core.pipeline.action.PipelineAction;

public class TestPipelineController implements PipelineController {
  private final Set<String> running = new LinkedHashSet<>();

  private final Set<String> starting = new LinkedHashSet<>();

  private final Map<String, Set<String>> subscribers;

  private final Map<String, Set<String>> publishers;

  private final Duration heartbeatInterval;

  private boolean canceling = false;

  private boolean failing = false;

  private ExecutionException failureReason = null;


  public TestPipelineController(Map<String, Set<String>> graph) {
    this(graph, Duration.ofMinutes(1));
  }

  public TestPipelineController(Map<String, Set<String>> graph, Duration heartbeatInterval) {
    this.subscribers = graph.entrySet().stream()
        .collect(toMap(Map.Entry::getKey, e -> unmodifiableSet(e.getValue())));
    this.publishers = graph.entrySet().stream()
        .flatMap(e -> e.getValue().stream().map(subscriber -> Map.entry(subscriber, e.getKey())))
        .collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toSet())));
    this.heartbeatInterval = heartbeatInterval;
  }


  @Override
  public List<PipelineAction> onPipelineStarted() {
    List<PipelineAction> result = new ArrayList<>();

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

    return result;
  }

  @Override
  public List<PipelineAction> onTaskStarted(String id) {
    starting.remove(id);

    running.add(id);

    List<PipelineAction> result = new ArrayList<>();

    if (canceling || failing) {
      result.add(PipelineAction.cancelTask(id));
    } else {
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
    }

    return result;
  }

  @Override
  public List<PipelineAction> onTaskCompleted(String id) {
    running.remove(id);

    List<PipelineAction> result = new ArrayList<>();

    for (Map.Entry<String, Set<String>> entry : subscribers.entrySet()) {
      final String taskId = entry.getKey();
      final Set<String> taskSubscribers = entry.getValue();
      if (running.contains(taskId) && !running.containsAll(taskSubscribers)) {
        // This is bad. A task has completed, but not all of its subscribers are completed yet.
        // So the task ended early. That's a failure. We can safely transition to FAILURE and
        // not check about FAILED because we know at least one task is still running.
        failing = true;
        failureReason = new ExecutionException(new IllegalStateException(
            "Task " + id + " completed, but not all subscribers are done"));
        break;
      }
    }

    if (failing) {
      for (String taskId : running)
        result.add(PipelineAction.cancelTask(taskId));
    } else if (running.isEmpty() && starting.isEmpty()) {
      result.add(PipelineAction.succeed());
    }

    return result;
  }

  @Override
  public List<PipelineAction> onTaskCancelled(String id) {
    running.remove(id);

    List<PipelineAction> result = new ArrayList<>();

    if (!canceling) {
      failing = true;
      failureReason = new ExecutionException(
          new IllegalStateException("Task " + id + " cancelled, but no cancel was requested"));
      for (String taskId : running)
        result.add(PipelineAction.cancelTask(taskId));
    }

    if (failing && running.isEmpty() && starting.isEmpty()) {
      result.add(PipelineAction.fail(failureReason));
    } else if (running.isEmpty() && starting.isEmpty()) {
      result.add(PipelineAction.cancel());
    }

    return result;
  }

  @Override
  public List<PipelineAction> onTaskFailed(String id, Throwable error) {
    running.remove(id);

    List<PipelineAction> result = new ArrayList<>();

    failing = true;
    failureReason = new ExecutionException(error);

    for (String taskId : running)
      result.add(PipelineAction.cancelTask(taskId));

    if (running.isEmpty() && starting.isEmpty())
      result.add(PipelineAction.fail(failureReason));

    return result;
  }

  @Override
  public List<PipelineAction> onHeartbeat() {
    return List.of();
  }

  @Override
  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }

  @Override
  public List<PipelineAction> onCancelRequested() {
    canceling = true;

    List<PipelineAction> result = new ArrayList<>();
    for (String taskId : running)
      result.add(PipelineAction.cancelTask(taskId));

    if (running.isEmpty() && starting.isEmpty())
      result.add(PipelineAction.cancel());

    return result;
  }

  @Override
  public void onPipelineCompleted() {}

  @Override
  public void onPipelineFailed(Throwable error) {}

  @Override
  public void onPipelineCancelled() {}
}
