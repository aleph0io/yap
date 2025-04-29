package io.aleph0.yap.messaging.core;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import java.util.Map;
import java.util.Objects;

public class MessageDefinition {
  public static MessageDefinition of(String body) {
    return new MessageDefinition(body);
  }

  public static MessageDefinition of(Map<String, String> attributes, String body) {
    return new MessageDefinition(attributes, body);
  }

  private final Map<String, String> attributes;
  private final String body;

  public MessageDefinition(String body) {
    this(Map.of(), body);
  }

  public MessageDefinition(Map<String, String> attributes, String body) {
    this.attributes = unmodifiableMap(requireNonNull(attributes, "attributes"));
    this.body = requireNonNull(body, "body");
  }

  public Map<String, String> attributes() {
    return attributes;
  }

  public String body() {
    return body;
  }

  @Override
  public int hashCode() {
    return Objects.hash(attributes, body);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MessageDefinition other = (MessageDefinition) obj;
    return Objects.equals(attributes, other.attributes) && Objects.equals(body, other.body);
  }

  @Override
  public String toString() {
    return "MessageDefinition [attributes=" + attributes + ", body=" + body + "]";
  }
}
