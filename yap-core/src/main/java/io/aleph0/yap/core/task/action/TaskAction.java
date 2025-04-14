package io.aleph0.yap.core.task.action;

import java.util.concurrent.ExecutionException;

public sealed interface TaskAction permits StartWorkerTaskAction, StopWorkerTaskAction,
    SucceedTaskAction, CancelTaskAction, FailTaskAction {
  static StartWorkerTaskAction newStartWorkerTaskAction() {
    return new StartWorkerTaskAction();
  }

  static StopWorkerTaskAction newStopWorkerTaskAction() {
    return new StopWorkerTaskAction();
  }

  static SucceedTaskAction newSucceedTask() {
    return new SucceedTaskAction();
  }

  static CancelTaskAction newCancelTask() {
    return new CancelTaskAction();
  }

  static FailTaskAction newFailTask(ExecutionException e) {
    return new FailTaskAction(e);
  }
}
