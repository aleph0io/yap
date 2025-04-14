package io.aleph0.yap.core.pipeline.action;

import static java.util.Objects.requireNonNull;

public record FailPipelineAction(Exception cause) implements PipelineAction {
  public FailPipelineAction {
    requireNonNull(cause);
  }
}
