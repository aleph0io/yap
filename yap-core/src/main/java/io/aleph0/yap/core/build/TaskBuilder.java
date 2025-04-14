package io.aleph0.yap.core.build;

public sealed interface TaskBuilder
    permits ProducerTaskBuilder, ProcessorTaskBuilder, ConsumerTaskBuilder {
  public String getId();
}
