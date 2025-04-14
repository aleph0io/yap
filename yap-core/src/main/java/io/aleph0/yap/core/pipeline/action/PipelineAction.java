package io.aleph0.yap.core.pipeline.action;

import java.util.concurrent.ExecutionException;

public sealed interface PipelineAction permits StartTaskPipelineAction, CancelTaskPipelineAction,
    SucceedPipelineAction, CancelPipelineAction, FailPipelineAction {
  public static StartTaskPipelineAction startTask(String id) {
    return new StartTaskPipelineAction(id);
  }

  public static CancelTaskPipelineAction cancelTask(String id) {
    return new CancelTaskPipelineAction(id);
  }

  public static SucceedPipelineAction succeed() {
    return new SucceedPipelineAction();
  }

  public static CancelPipelineAction cancel() {
    return new CancelPipelineAction();
  }

  public static FailPipelineAction fail(ExecutionException cause) {
    return new FailPipelineAction(cause);
  }

}
