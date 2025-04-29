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
