package io.aleph0.yap.core;

import io.aleph0.yap.core.task.TaskController;

public interface Measureable<M> {
  /**
   * Non-destructive check of the metrics. This should be used to check the state of metrics without
   * clearing them, for example in a {@link TaskController}.
   * 
   * @return the metrics
   */
  public M checkMetrics();

  /**
   * Destructive read of the metrics. This should be used to check and reset the state of metrics,
   * for example by the metrics checking thread.
   *  
   * @return the metrics
   */
  public M flushMetrics();
}
