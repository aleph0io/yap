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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.messaging.core.Acknowledgeable.AcknowledgementListener;
import io.aleph0.yap.messaging.core.worker.SplitGateProcessorWorker;

class SplitGateProcessorWorkerTest {

  private Function<String, String> mapper;
  private Function<String, Collection<String>> splitter;
  private SplitGateProcessorWorker<String, String> worker;
  private TestSource source;
  private TestSink sink;

  @BeforeEach
  public void setup() {
    mapper = input -> input + "-mapped";
    splitter = input -> {
      if (input.contains("split")) {
        return List.of(input + "-0", input + "-1");
      } else {
        return Collections.emptyList();
      }
    };

    worker = new SplitGateProcessorWorker<>(mapper, splitter);
    source = new TestSource();
    sink = new TestSink();
  }

  @Test
  public void testNoSplitCase() throws InterruptedException {
    // Message that won't be split
    TestMessage originalMessage = new TestMessage("1", "x");
    source.addMessage(originalMessage);

    // Process the message
    worker.process(source, sink);

    // Verify exactly one message sent downstream
    assertThat(sink.getMessages()).hasSize(1);

    // Verify metrics show no gated messages
    assertThat(worker.checkMetrics().gated()).isEqualTo(0);

    // Verify the message body was mapped correctly
    Message<String> outputMsg = sink.getMessages().get(0);
    assertThat(outputMsg.id()).isEqualTo("1");
  }

  @Test
  public void testSplitCaseWithAcks() throws InterruptedException {
    // Message that will be split into 2 messages
    TestMessage originalMessage = new TestMessage("2", "split");
    source.addMessage(originalMessage);

    // Process the message
    worker.process(source, sink);

    // Should have 2 split messages sent but not the original yet
    assertThat(sink.getMessages()).hasSize(2);
    assertThat(worker.checkMetrics().gated()).isEqualTo(1);

    // Verify split messages have the right IDs and bodies
    final Message<String> splitMessage0 = sink.getMessages().remove(0);
    assertThat(splitMessage0.id()).isEqualTo("2-0");
    assertThat(splitMessage0.body()).isEqualTo("split-0");

    final Message<String> splitMessage1 = sink.getMessages().remove(0);
    assertThat(splitMessage1.id()).isEqualTo("2-1");
    assertThat(splitMessage1.body()).isEqualTo("split-1");

    // Acknowledge both split messages
    AcknowledgementListener listener = mock(AcknowledgementListener.class);
    splitMessage0.ack(listener);
    splitMessage1.ack(listener);

    // Verify original message is now downstream
    assertThat(sink.getMessages()).hasSize(1);
    final Message<String> gatedMessage = sink.getMessages().get(0);
    assertThat(gatedMessage.id()).isEqualTo("2");
    assertThat(gatedMessage.body()).isEqualTo("split-mapped");
  }

  @Test
  public void testSplitCaseWithNack() throws InterruptedException {
    // Message that will be split
    TestMessage originalMessage = new TestMessage("3", "split");
    source.addMessage(originalMessage);

    // Process the message
    worker.process(source, sink);

    // Verify two split messages
    assertThat(sink.getMessages()).hasSize(2);
    assertThat(worker.checkMetrics().gated()).isEqualTo(1);

    // Verify split messages have the right IDs and bodies
    final Message<String> splitMessage0 = sink.getMessages().remove(0);
    assertThat(splitMessage0.id()).isEqualTo("3-0");
    assertThat(splitMessage0.body()).isEqualTo("split-0");

    final Message<String> splitMessage1 = sink.getMessages().remove(0);
    assertThat(splitMessage1.id()).isEqualTo("3-1");
    assertThat(splitMessage1.body()).isEqualTo("split-1");

    // Nack one split message
    AcknowledgementListener listener = mock(AcknowledgementListener.class);
    splitMessage0.nack(listener);

    // Verify original message gets nacked and the counter is decremented
    assertThat(originalMessage.isNacked()).isTrue();
    assertThat(worker.checkMetrics().gated()).isEqualTo(0);

    // The original message should not be put to sink after nacking
    assertThat(sink.getMessages()).hasSize(0);
  }

  @Test
  public void testFlushMetrics() throws InterruptedException {
    TestMessage msg1 = new TestMessage("4", "split");
    TestMessage msg2 = new TestMessage("5", "split");
    source.addMessage(msg1);
    source.addMessage(msg2);

    // Process the messages
    worker.process(source, sink);

    // Should have 4 split messages and 2 gated messages
    assertThat(sink.getMessages()).hasSize(4);
    assertThat(worker.checkMetrics().gated()).isEqualTo(2);

    // Test flushMetrics returns correct value
    assertThat(worker.flushMetrics()).isEqualTo(new SplitGateProcessorWorker.Metrics(2));
  }

  // Test helper classes

  public static class TestMessage implements Message<String> {
    private final String id;
    private final String body;
    private final Map<String, String> attributes = new HashMap<>();
    private boolean acked = false;
    private boolean nacked = false;

    TestMessage(String id, String body) {
      this.id = id;
      this.body = body;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public String body() {
      return body;
    }

    @Override
    public Map<String, String> attributes() {
      return attributes;
    }

    @Override
    public void ack(AcknowledgementListener listener) {
      acked = true;
      listener.onSuccess();
    }

    @Override
    public void nack(AcknowledgementListener listener) {
      nacked = true;
      listener.onSuccess();
    }

    boolean isAcked() {
      return acked;
    }

    boolean isNacked() {
      return nacked;
    }
  }

  public static class TestSource implements Source<Message<String>> {
    private final Queue<Message<String>> messages = new LinkedList<>();

    void addMessage(Message<String> msg) {
      messages.add(msg);
    }

    @Override
    public Message<String> take() {
      return messages.poll();
    }

    @Override
    public Message<String> tryTake() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Message<String> take(Duration timeout) throws InterruptedException, TimeoutException {
      throw new UnsupportedOperationException("not implemented");
    }
  }

  public static class TestSink implements Sink<Message<String>> {
    private final List<Message<String>> messages = new ArrayList<>();

    @Override
    public void put(Message<String> message) {
      messages.add(message);
    }

    List<Message<String>> getMessages() {
      return messages;
    }
  }
}
