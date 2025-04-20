package io.aleph0.yap.core.pipeline;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import io.aleph0.yap.core.Pipeline;
import io.aleph0.yap.core.pipeline.action.PipelineAction;

/**
 * The PipelineController is responsible for controlling the lifecycle of a {@link Pipeline}. The
 * {@link PipelineManager} will call the methods of this interface to notify the PipelineController
 * of various events that occur during the lifecycle of a pipeline, and the PipelineController will
 * return the list of {@link PipelineAction}s to perform in response to those events. The
 * TaskController is generally free to decide how to respond to these events, for example
 * implementing task-level retry logic or implementing a circuit-breaker pattern, but to ensure
 * correctness, it must conform to the high-level task lifecycle defined by the
 * {@code PipelineManager}.
 */
public interface PipelineController {
  /**
   * Called when the pipeline has been started. Called as the first event only.
   * 
   * @return the next actions to perform
   */
  public List<PipelineAction> onPipelineStarted();

  /**
   * Called when a task has been started. Called for each task as the first event.
   * 
   * @param id the id of the task
   * @return the next actions to perform
   */
  public List<PipelineAction> onTaskStarted(String id);

  /**
   * Called when a task has been completed. No more events will be sent for this task.
   * 
   * @param id the id of the task
   * @return the next actions to perform
   */
  public List<PipelineAction> onTaskCompleted(String id);

  /**
   * Called when a task has been cancelled. No more events will be sent for this task.
   * 
   * @param id the id of the task
   * @return the next actions to perform
   */
  public List<PipelineAction> onTaskCancelled(String id);

  /**
   * Called when a task has failed. No more events will be sent for this task.
   * 
   * @param id the id of the task
   * @param error the error that caused the task to fail
   * @return the next actions to perform
   */
  public List<PipelineAction> onTaskFailed(String id, Throwable error);

  /**
   * Called when no lifecycle events have occurred within {@link #getHeartbeatInterval() the
   * heartbeat interval}. This ensures that the controller has periodic access to execute actions
   * even if no lifecycle events are occurring, for example to time out tasks or to perform
   * maintenance tasks.
   * 
   * @return the next action to perform
   */
  public List<PipelineAction> onHeartbeat();

  /**
   * The maximum amount of time to wait for a lifecycle event, after which {@link #onHeartbeat()}
   * will be called.
   */
  public Duration getHeartbeatInterval();

  /**
   * Called when the pipeline has been requested to be cancelled. This is a request, not a command,
   * and the pipeline may choose to ignore it.
   * 
   * @return the next actions to perform
   */
  public List<PipelineAction> onCancelRequested();

  /**
   * Called when the pipeline has been completed. This is the last event that will be called.
   */
  public void onPipelineCompleted();

  /**
   * Called when the pipeline has failed. This is the last event that will be called.
   */
  public void onPipelineFailed(ExecutionException cause);

  /**
   * Called when the pipeline has been cancelled. This is the last event that will be called.
   */
  public void onPipelineCancelled();
}
