package io.aleph0.yap.core.worker;

import io.aleph0.yap.core.ConsumerWorker;
import io.aleph0.yap.core.Measureable;

public interface ConsumerWorkerFactory<InputT, MetricsT> extends Measureable<MetricsT> {
  public ConsumerWorker<InputT> newConsumerWorker();
}
