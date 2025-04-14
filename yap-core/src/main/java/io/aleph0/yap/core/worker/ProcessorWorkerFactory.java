package io.aleph0.yap.core.worker;

import io.aleph0.yap.core.Measureable;
import io.aleph0.yap.core.ProcessorWorker;

public interface ProcessorWorkerFactory<InputT, OutputT, MetricsT> extends Measureable<MetricsT> {
  public ProcessorWorker<InputT, OutputT> newProcessorWorker();
}
