package io.aleph0.yap.core.pipeline;

import io.aleph0.yap.core.Pipeline;

@FunctionalInterface
public interface PipelineWrapper {
  public Pipeline wrapPipeline(Pipeline pipeline);
}
