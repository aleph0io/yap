package io.aleph0.yap.core.pipeline;

import static java.util.Objects.requireNonNull;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import io.aleph0.yap.core.Pipeline;

public class DefaultPipeline implements Pipeline {
  private final List<LifecycleListener> lifecycleListeners = new CopyOnWriteArrayList<>();

  private final PipelineManager manager;
  private volatile boolean cancelled = false;
  private volatile Future<?> future;

  public DefaultPipeline(PipelineManager manager) {
    this.manager = requireNonNull(manager);
    this.manager.addLifecycleListener(new PipelineManagerLifecycleListenerAdapter());
  }

  @Override
  public void addLifecycleListener(LifecycleListener listener) {
    if (listener == null)
      throw new NullPointerException();
    lifecycleListeners.add(listener);
  }

  @Override
  public void removeLifecycleListener(LifecycleListener listener) {
    lifecycleListeners.remove(listener);
  }

  @Override
  public int getId() {
    return manager.getId();
  }

  public void start() {
    if (cancelled) {
      // If the pipeline is cancelled, we don't want to start it.
      return;
    }
    if (future != null) {
      // If the pipeline is already started, we don't want to start it again.
      return;
    }
    future = Executors.newVirtualThreadPerTaskExecutor().submit(() -> {
      try {
        manager.run();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  public void cancel() {
    cancelled = true;
    if (future != null)
      future.cancel(true);
  }

  public void await() throws InterruptedException, ExecutionException, CancellationException {
    if (cancelled && future == null)
      throw new CancellationException();
    future.get();
  }

  @Override
  public Pipeline.Metrics checkMetrics() {
    final PipelineManager.Metrics metrics = manager.checkMetrics();
    return new Pipeline.Metrics(metrics.id(), metrics.phase(), metrics.state(), metrics.tasks());
  }

  @Override
  public Pipeline.Metrics flushMetrics() {
    final Pipeline.Metrics result = checkMetrics();
    manager.flushMetrics();
    return result;
  }

  private class PipelineManagerLifecycleListenerAdapter
      implements PipelineManager.LifecycleListener {
    @Override
    public void onPipelineStarted(int pipeline) {
      notifyLifecycleListeners(l -> l.onPipelineStarted(pipeline));
    }

    @Override
    public void onPipelineTaskStarted(int pipeline, String task) {
      notifyLifecycleListeners(l -> l.onPipelineTaskStarted(pipeline, task));
    }

    @Override
    public void onPipelineTaskWorkerStarted(int pipeline, String task, int worker) {
      notifyLifecycleListeners(l -> l.onPipelineTaskWorkerStarted(pipeline, task, worker));
    }

    @Override
    public void onPipelineTaskWorkerStopRequested(int pipeline, String task, int worker) {
      notifyLifecycleListeners(l -> l.onPipelineTaskWorkerStopRequested(pipeline, task, worker));
    }

    @Override
    public void onPipelineTaskWorkerStopped(int pipeline, String task, int worker) {
      notifyLifecycleListeners(l -> l.onPipelineTaskWorkerStopped(pipeline, task, worker));
    }

    @Override
    public void onPipelineTaskWorkerCompletedNormally(int pipeline, String task, int worker) {
      notifyLifecycleListeners(
          l -> l.onPipelineTaskWorkerCompletedNormally(pipeline, task, worker));
    }

    @Override
    public void onPipelineTaskWorkerCompletedExceptionally(int pipeline, String task, int worker,
        Throwable cause) {
      notifyLifecycleListeners(
          l -> l.onPipelineTaskWorkerCompletedExceptionally(pipeline, task, worker, cause));
    }

    @Override
    public void onPipelineTaskCancelRequested(int pipeline, String task, int worker) {
      notifyLifecycleListeners(l -> l.onPipelineTaskCancelRequested(pipeline, task, worker));
    }

    @Override
    public void onPipelineTaskCompleted(int pipeline, String task) {
      notifyLifecycleListeners(l -> l.onPipelineTaskCompleted(pipeline, task));
    }

    @Override
    public void onPipelineTaskCancelled(int pipeline, String task) {
      notifyLifecycleListeners(l -> l.onPipelineTaskCancelled(pipeline, task));
    }

    @Override
    public void onPipelineTaskFailed(int pipeline, String task, Throwable cause) {
      notifyLifecycleListeners(l -> l.onPipelineTaskFailed(pipeline, task, cause));
    }

    @Override
    public void onPipelineCancelRequested(int pipeline) {
      notifyLifecycleListeners(l -> l.onPipelineCancelRequested(pipeline));
    }

    @Override
    public void onPipelineCompleted(int pipeline) {
      notifyLifecycleListeners(l -> l.onPipelineCompleted(pipeline));
    }

    @Override
    public void onPipelineCancelled(int pipeline) {
      notifyLifecycleListeners(l -> l.onPipelineCancelled(pipeline));
    }

    @Override
    public void onPipelineFailed(int pipeline, Throwable cause) {
      notifyLifecycleListeners(l -> l.onPipelineFailed(pipeline, cause));
    }
  }

  private void notifyLifecycleListeners(Consumer<LifecycleListener> event) {
    for (LifecycleListener listener : lifecycleListeners)
      event.accept(listener);
  }
}
