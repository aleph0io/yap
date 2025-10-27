/*-
 * =================================LICENSE_START==================================
 * yap-messaging-gcp
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
package io.aleph0.yap.messaging.gcp.worker;


import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.google.pubsub.v1.PubsubMessage;
import io.aleph0.yap.messaging.core.Acknowledgeable.AcknowledgementListener;
import io.aleph0.yap.messaging.gcp.worker.PubsubFirehoseProducerWorker.PubsubFirehoseMessage;

/**
 * Test various attributes of the PubsubFirehoseProducerWorker.PubsubFirehoseMessage class,
 * especially (a) mutual exclusivity of ack and nack; (b) idempotence; and (c) tolerance for
 * concurrent invocation.
 */
public class PubsubFirehoseProducerWorkerMessageTest {
  public ScheduledExecutorService executor;


  @BeforeEach
  public void setup() {
    executor = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterEach
  public void cleanup() throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(30, TimeUnit.SECONDS);
  }

  @Test
  public void serialAckAckSuccessTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      l.onSuccess();
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      throw new UnsupportedOperationException("nack not under test");
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final CountDownLatch latch = new CountDownLatch(1);
    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        throw new UnsupportedOperationException("onFailure not under test");
      }
    });

    latch.await();

    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success");
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage());
      }
    });

    final String result = queue.poll();

    assertThat(result).isEqualTo("success");
  }

  @Test
  public void serialAckAckFailureTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      l.onFailure(new RuntimeException("simulated failure"));
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      throw new UnsupportedOperationException("nack not under test");
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final CountDownLatch latch = new CountDownLatch(1);
    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        throw new UnsupportedOperationException("onSuccess not under test");
      }

      @Override
      public void onFailure(Throwable t) {
        latch.countDown();
      }
    });

    latch.await();

    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success");
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage());
      }
    });

    final String result = queue.poll();

    assertThat(result).isEqualTo("simulated failure");
  }

  @Test
  public void serialAckNackTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      l.onSuccess();
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      throw new UnsupportedOperationException("nack not under test");
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final CountDownLatch latch = new CountDownLatch(1);
    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        throw new UnsupportedOperationException("onFailure not under test");
      }
    });

    latch.await();

    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success");
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage());
      }
    });

    final String result = queue.poll();

    assertThat(result).isEqualTo("message already acked");
  }

  @Test
  public void concurrentAckAckSuccessTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      executor.schedule(() -> {
        l.onSuccess();
      }, 10, TimeUnit.MILLISECONDS);
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      throw new UnsupportedOperationException("nack not under test");
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        // Life is good!
        queue.offer("success1");
      }

      @Override
      public void onFailure(Throwable t) {
        throw new UnsupportedOperationException("onFailure not under test");
      }
    });

    final CountDownLatch latch = new CountDownLatch(1);
    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success2");
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage());
        latch.countDown();
      }
    });

    latch.await();

    final List<String> result = new ArrayList<>();
    queue.drainTo(result);

    assertThat(result).containsExactly("success1", "success2");
  }

  @Test
  public void concurrentAckAckFailureTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      executor.schedule(() -> {
        l.onFailure(new RuntimeException("simulated failure"));
      }, 10, TimeUnit.MILLISECONDS);
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      throw new UnsupportedOperationException("nack not under test");
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        throw new UnsupportedOperationException("onFailure not under test");
      }

      @Override
      public void onFailure(Throwable t) {
        // Life is good!
        queue.offer(t.getMessage() + "1");
      }
    });

    final CountDownLatch latch = new CountDownLatch(1);
    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success2");
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage() + "2");
        latch.countDown();
      }
    });

    latch.await();

    final List<String> result = new ArrayList<>();
    queue.drainTo(result);

    assertThat(result).containsExactly("simulated failure1", "simulated failure2");
  }

  @Test
  public void concurrentAckNackTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      executor.schedule(() -> {
        l.onSuccess();
      }, 10, TimeUnit.MILLISECONDS);
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      throw new UnsupportedOperationException("nack not under test");
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final CountDownLatch latch = new CountDownLatch(1);
    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        // Life is good!
        queue.offer("success1");
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        throw new UnsupportedOperationException("onFailure not under test");
      }
    });

    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success2");
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage());
      }
    });

    latch.await();

    final List<String> result = new ArrayList<>();
    queue.drainTo(result);

    assertThat(result).containsExactly("message already acked", "success1");
  }

  @Test
  public void serialNackNackSuccessTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      throw new UnsupportedOperationException("ack not under test");
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      l.onSuccess();
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final CountDownLatch latch = new CountDownLatch(1);
    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        throw new UnsupportedOperationException("onFailure not under test");
      }
    });

    latch.await();

    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success");
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage());
      }
    });

    final String result = queue.poll();

    assertThat(result).isEqualTo("success");
  }

  @Test
  public void serialNackNackFailureTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      throw new UnsupportedOperationException("ack not under test");
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      l.onFailure(new RuntimeException("simulated failure"));
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final CountDownLatch latch = new CountDownLatch(1);
    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        throw new UnsupportedOperationException("onSuccess not under test");
      }

      @Override
      public void onFailure(Throwable t) {
        latch.countDown();
      }
    });

    latch.await();

    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success");
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage());
      }
    });

    final String result = queue.poll();

    assertThat(result).isEqualTo("simulated failure");
  }

  @Test
  public void serialNackAckTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      throw new UnsupportedOperationException("ack not under test");
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      l.onSuccess();
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final CountDownLatch latch = new CountDownLatch(1);
    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        throw new UnsupportedOperationException("onFailure not under test");
      }
    });

    latch.await();

    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success");
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage());
      }
    });

    final String result = queue.poll();

    assertThat(result).isEqualTo("message already nacked");
  }

  @Test
  public void concurrentNackNackSuccessTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      throw new UnsupportedOperationException("ack not under test");
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      executor.schedule(() -> {
        l.onSuccess();
      }, 10, TimeUnit.MILLISECONDS);
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        // Life is good!
        queue.offer("success1");
      }

      @Override
      public void onFailure(Throwable t) {
        throw new UnsupportedOperationException("onFailure not under test");
      }
    });

    final CountDownLatch latch = new CountDownLatch(1);
    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success2");
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage());
        latch.countDown();
      }
    });

    latch.await();

    final List<String> result = new ArrayList<>();
    queue.drainTo(result);

    assertThat(result).containsExactly("success1", "success2");
  }

  @Test
  public void concurrentNackNackFailureTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      throw new UnsupportedOperationException("ack not under test");
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      executor.schedule(() -> {
        l.onFailure(new RuntimeException("simulated failure"));
      }, 10, TimeUnit.MILLISECONDS);
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        throw new UnsupportedOperationException("onSuccess not under test");
      }

      @Override
      public void onFailure(Throwable t) {
        // Life is good!
        queue.offer(t.getMessage() + "1");
      }
    });

    final CountDownLatch latch = new CountDownLatch(1);
    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success2");
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage() + "2");
        latch.countDown();
      }
    });

    final List<String> result = new ArrayList<>();
    queue.drainTo(result);

    assertThat(result).containsExactly("simulated failure1", "simulated failure2");
  }

  @Test
  public void concurrentNackAckTest() throws InterruptedException {
    final PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(com.google.protobuf.ByteString.copyFromUtf8("test")).build();

    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> acker = (m, l) -> {
      throw new UnsupportedOperationException("ack not under test");
    };
    final BiConsumer<PubsubFirehoseMessage, AcknowledgementListener> nacker = (m, l) -> {
      executor.schedule(() -> {
        l.onSuccess();
      }, 10, TimeUnit.MILLISECONDS);
    };

    final PubsubFirehoseProducerWorker.PubsubFirehoseMessage unit =
        new PubsubFirehoseProducerWorker.PubsubFirehoseMessage(psm, acker, nacker);

    final CountDownLatch latch = new CountDownLatch(1);
    final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    unit.nack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        // Life is good!
        queue.offer("success1");
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        throw new UnsupportedOperationException("onFailure not under test");
      }
    });

    unit.ack(new AcknowledgementListener() {
      @Override
      public void onSuccess() {
        queue.offer("success2");
      }

      @Override
      public void onFailure(Throwable t) {
        queue.offer(t.getMessage());
      }
    });

    latch.await();

    final List<String> result = new ArrayList<>();
    queue.drainTo(result);

    assertThat(result).containsExactly("message already nacked", "success1");
  }
}
