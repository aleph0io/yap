package io.aleph0.yap.core;

@FunctionalInterface
public interface ProducerWorker<OutputT> {
  public void produce(Sink<OutputT> sink) throws Exception;
}
