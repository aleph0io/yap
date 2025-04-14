package io.aleph0.yap.core.worker;

import io.aleph0.yap.core.Measureable;
import io.aleph0.yap.core.ProducerWorker;

public interface ProducerWorkerFactory<OutputT, MetricsT> extends Measureable<MetricsT> {
  public ProducerWorker<OutputT> newProducerWorker();
}
