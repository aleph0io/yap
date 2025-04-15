package io.aleph0.yap.core.build;

import java.util.Map;
import java.util.Set;
import io.aleph0.yap.core.pipeline.PipelineController;

public interface PipelineControllerBuilder {
  public PipelineController build(Map<String, Set<String>> graph);
}
