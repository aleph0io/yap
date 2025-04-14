package io.aleph0.yap.core;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public interface Source<T> {
  public T tryTake();

  public T take(Duration timeout) throws InterruptedException, TimeoutException;

  public T take() throws InterruptedException;
}
