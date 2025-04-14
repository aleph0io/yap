package io.aleph0.yap.core;

@FunctionalInterface
public interface ConsumerWorker<InputT> {
  public void consume(Source<InputT> source) throws Exception;
}
