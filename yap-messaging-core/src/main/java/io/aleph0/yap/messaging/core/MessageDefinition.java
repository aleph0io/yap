/*-
 * =================================LICENSE_START==================================
 * yap-messaging-core
 * ====================================SECTION=====================================
 * Copyright (C) 2025 aleph0
 * ====================================SECTION=====================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ==================================LICENSE_END===================================
 */
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
