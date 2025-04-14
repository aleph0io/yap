package io.aleph0.yap.core.transport.channel;

import static java.util.Objects.requireNonNull;
import io.aleph0.yap.core.transport.Channel;
import io.aleph0.yap.core.transport.Channel.Binding;

public class DefaultChannel<T> implements Channel<T> {
  private Channel.Binding<T> binding;

  @Override
  public void bind(Channel.Binding<T> binding) {
    this.binding = requireNonNull(binding);
  }

  @Override
  public boolean tryPublish(T message) {
    if (binding == null)
      throw new IllegalStateException("not bound yet");
    return binding.tryPublish(message);
  }

  @Override
  public void publish(T message) throws InterruptedException {
    if (binding == null)
      throw new IllegalStateException("not bound yet");
    binding.publish(message);
  }

  @Override
  public void close() {
    if (binding == null)
      throw new IllegalStateException("not bound yet");
    binding.close();
  }
}
