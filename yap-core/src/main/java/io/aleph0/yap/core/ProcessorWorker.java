package io.aleph0.yap.core;

@FunctionalInterface
public interface ProcessorWorker<InputT, OutputT> {
  public void process(Source<InputT> source, Sink<OutputT> sink) throws Exception;
}
