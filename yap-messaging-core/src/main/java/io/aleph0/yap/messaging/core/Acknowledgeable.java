package io.aleph0.yap.messaging.core;

public interface Acknowledgeable {
  public static interface AcknowledgementListener {
    public void onSuccess();

    public void onFailure(Throwable t);
  }

  public void ack(AcknowledgementListener listener);

  public void nack(AcknowledgementListener listener);
}
