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

import java.util.Map;


public interface Message<T> extends Acknowledgeable {
  /**
   * The message ID. This is a unique identifier for the message. It should be unique across all
   * messages in the system, but does not need to be globally unique. Re-delivery of the same
   * message should result in the same ID.
   */
  public String id();

  /**
   * Optional key-value metadata associated with the message.
   */
  public Map<String, String> attributes();

  /**
   * The content of the message
   */
  public T body();
}
