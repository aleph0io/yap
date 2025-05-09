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

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import io.aleph0.yap.core.build.PipelineBuilder;
import io.aleph0.yap.core.pipeline.MonitoredPipeline;

public class PipelineTest {
  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void smokeTest() throws CancellationException, InterruptedException, ExecutionException {
    // This really should run in much less than 1 second, but let's not be flaky.
    // This is not a performance test, just a smoke test.

    final PipelineBuilder pb = Pipeline.builder();

    final var producer = pb.addProducer("producer", (Sink<Integer> sink) -> {
      for (int i = 1; i <= 10; i++) {
        sink.put(i);
      }
    });

    final var add1 = pb.addProcessor("add1", (Source<Integer> source, Sink<Integer> sink) -> {
      for (Integer n = source.take(); n != null; n = source.take()) {
        sink.put(n + 1);
      }
    });
    producer.addSubscriber(add1);

    final var add3 = pb.addProcessor("add3", (Source<Integer> source, Sink<Integer> sink) -> {
      for (Integer n = source.take(); n != null; n = source.take()) {
        sink.put(n + 3);
      }
    });
    producer.addSubscriber(add3);

    final Set<Integer> results = ConcurrentHashMap.newKeySet();
    final var consumer = pb.addConsumer("consumer", (Source<Integer> source) -> {
      for (Integer n = source.take(); n != null; n = source.take()) {
        results.add(n);
      }
    });
    add1.addSubscriber(consumer);
    add3.addSubscriber(consumer);

    pb.addWrapper(MonitoredPipeline.newWrapper()).buildAndStart().await();

    assertThat(results).isEqualTo(Stream.of(
        // add1
        2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
        // add3
        4, 5, 6, 7, 8, 9, 10, 11, 12, 13).collect(toSet()));
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void failureTest() {

    final PipelineBuilder pb = Pipeline.builder();

    final var producer = pb.addProducer("producer", (Sink<Integer> sink) -> {
      for (int i = 1; i <= 10; i++) {
        sink.put(i);
      }
    });

    final var add1 = pb.addProcessor("add1", (Source<Integer> source, Sink<Integer> sink) -> {
      for (Integer n = source.take(); n != null; n = source.take()) {
        if (n == 1)
          throw new RuntimeException("simulated failure");
        sink.put(n + 1);
      }
    });
    producer.addSubscriber(add1);

    final var add3 = pb.addProcessor("add3", (Source<Integer> source, Sink<Integer> sink) -> {
      for (Integer n = source.take(); n != null; n = source.take()) {
        sink.put(n + 3);
      }
    });
    producer.addSubscriber(add3);

    final Set<Integer> results = ConcurrentHashMap.newKeySet();
    final var consumer = pb.addConsumer("consumer", (Source<Integer> source) -> {
      for (Integer n = source.take(); n != null; n = source.take()) {
        results.add(n);
      }
    });
    add1.addSubscriber(consumer);
    add3.addSubscriber(consumer);

    assertThatThrownBy(() -> pb.addWrapper(MonitoredPipeline.newWrapper()).buildAndStart().await())
        .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("simulated failure");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void cycleTest() {
    final PipelineBuilder pb = Pipeline.builder();

    final var producer = pb.addProducer("producer", (Sink<Integer> sink) -> {
      for (int i = 1; i <= 10; i++) {
        sink.put(i);
      }
    });

    final var add1 = pb.addProcessor("add1", (Source<Integer> source, Sink<Integer> sink) -> {
      for (Integer n = source.take(); n != null; n = source.take()) {
        sink.put(n + 1);
      }
    });
    producer.addSubscriber(add1);

    // This creates a cycle
    add1.addSubscriber(add1);

    assertThatThrownBy(() -> pb.addWrapper(MonitoredPipeline.newWrapper()).buildAndStart().await())
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cycle");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void disconnectedTest() {
    final PipelineBuilder pb = Pipeline.builder();

    final var producer = pb.addProducer("producer", (Sink<Integer> sink) -> {
      for (int i = 1; i <= 10; i++) {
        sink.put(i);
      }
    });

    final var add1 = pb.addProcessor("add1", (Source<Integer> source, Sink<Integer> sink) -> {
      for (Integer n = source.take(); n != null; n = source.take()) {
        sink.put(n + 1);
      }
    });

    assertThatThrownBy(() -> pb.addWrapper(MonitoredPipeline.newWrapper()).buildAndStart().await())
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("subscriber");
  }
}
