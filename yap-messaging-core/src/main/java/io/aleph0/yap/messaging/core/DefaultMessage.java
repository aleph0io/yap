package io.aleph0.yap.messaging.core;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import java.util.Map;

public abstract class DefaultMessage implements Message {
  private final Map<String, String> attributes;
  private final String body;

  public DefaultMessage(Map<String, String> attributes, String body) {
    this.attributes = unmodifiableMap(requireNonNull(attributes, "attributes"));
    this.body = requireNonNull(body, "body");
  }

  @Override
  public Map<String, String> attributes() {
    return attributes;
  }

  @Override
  public String body() {
    return body;
  }
}
