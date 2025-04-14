package io.aleph0.yap.core.task.action;

import static java.util.Objects.requireNonNull;
import java.util.concurrent.ExecutionException;

public record FailTaskAction(ExecutionException cause) implements TaskAction {
  public FailTaskAction {
    requireNonNull(cause);
  }
}
