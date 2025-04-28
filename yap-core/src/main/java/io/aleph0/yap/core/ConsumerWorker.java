/*-
 * =================================LICENSE_START==================================
 * yap-core
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
package io.aleph0.yap.core;

/**
 * A worker that consumes inputs via a {@link Source source}.
 * 
 * <p>
 * This worker is not {@link Measureable measured}, which is to say it does not produce any metrics.
 * 
 * @param <InputT> the type of the input
 */
@FunctionalInterface
public interface ConsumerWorker<InputT> {
  public void consume(Source<InputT> source) throws Exception;
}
