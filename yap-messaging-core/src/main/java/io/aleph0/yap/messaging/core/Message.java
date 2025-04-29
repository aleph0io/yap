package io.aleph0.yap.messaging.core;

import java.util.Map;


public interface Message extends Acknowledgeable {
  public Map<String, String> attributes();

  public String body();
}
