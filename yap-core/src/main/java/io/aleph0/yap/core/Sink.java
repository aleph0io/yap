package io.aleph0.yap.core;

@FunctionalInterface
public interface Sink<T> {
  public void put(T value) throws InterruptedException;
}
