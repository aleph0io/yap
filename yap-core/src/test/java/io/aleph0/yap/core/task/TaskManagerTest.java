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
package io.aleph0.yap.core.task;

import static java.util.Collections.unmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import io.aleph0.yap.core.task.TaskManager.TaskState;
import io.aleph0.yap.core.task.TaskManager.WorkerBody;
import io.aleph0.yap.core.task.TaskManager.WorkerBodyFactory;
import io.aleph0.yap.core.task.TaskManager.WorkerCompletedEvent;
import io.aleph0.yap.core.task.TaskManager.WorkerEvent;
import io.aleph0.yap.core.task.TaskManager.WorkerFailedEvent;
import io.aleph0.yap.core.task.TaskManager.WorkerStartedEvent;
import io.aleph0.yap.core.task.TaskManager.WorkerStoppedEvent;
import io.aleph0.yap.core.task.action.FailTaskAction;
import io.aleph0.yap.core.task.action.TaskAction;
import io.aleph0.yap.core.transport.Channel;
import io.aleph0.yap.core.transport.Queue;
import io.aleph0.yap.core.transport.Topic;
import io.aleph0.yap.core.transport.channel.DefaultChannel;
import io.aleph0.yap.core.transport.queue.DefaultQueue;
import io.aleph0.yap.core.transport.topic.DefaultTopic;
import io.aleph0.yap.core.util.NoMetrics;

class TaskManagerTest {
  public final String id = "test";
  public final Set<String> subscribers = Set.of("alpha", "bravo");
  public final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

  public Channel<String> channel;
  public Queue<String> queue;
  public Topic<String> topic;

  @BeforeEach
  public void setup() {
    channel = new DefaultChannel<>();
    queue = new DefaultQueue<>(10, List.of(channel));
    topic = new DefaultTopic<>(List.of(channel));

  }

  public WorkerBodyFactory<NoMetrics> newWorkerBodyFactory(WorkerBody body) {
    return newWorkerBodyFactory(() -> body);
  }

  public WorkerBodyFactory<NoMetrics> newWorkerBodyFactory(Supplier<WorkerBody> supplier) {
    return new WorkerBodyFactory<>() {
      @Override
      public WorkerBody newWorkerBody() {
        return supplier.get();
      }

      @Override
      public NoMetrics checkMetrics() {
        return NoMetrics.INSTANCE;
      }

      @Override
      public NoMetrics flushMetrics() {
        return NoMetrics.INSTANCE;
      }
    };
  }

  public static record EventOrAction(WorkerEvent event, TaskAction action) {
    public static EventOrAction of(WorkerEvent event) {
      return new EventOrAction(event, null);
    }

    public static EventOrAction of(TaskAction action) {
      return new EventOrAction(null, action);
    }
  }

  public static class RecordingTaskManager<MetricsT> extends TaskManager<MetricsT> {
    public RecordingTaskManager(String id, Set<String> subscribers, ExecutorService executor,
        TaskController controller, WorkerBodyFactory<MetricsT> workerBodyFactory, Queue<?> queue,
        Topic<?> topic) {
      super(id, subscribers, executor, controller, workerBodyFactory, queue, topic);
    }

    private final List<EventOrAction> eoas = new ArrayList<>();

    @Override
    List<TaskAction> handleWorkerEvent(WorkerEvent event) {
      eoas.add(EventOrAction.of(event));
      return super.handleWorkerEvent(event);
    }

    @Override
    void performTaskAction(TaskAction action) throws InterruptedException {
      eoas.add(EventOrAction.of(action));
      super.performTaskAction(action);
    }

    public List<EventOrAction> getEventsAndActions() {
      return unmodifiableList(eoas);
    }
  }

  @Test
  @Timeout(5)
  void givenSuccessfulWorkerWithOneConcurrency_whenRun_thenGetExpectedEventsAndActions()
      throws Exception {
    final TaskController controller = new TestTaskController(1);

    final WorkerBodyFactory<NoMetrics> workerBodyFactory = newWorkerBodyFactory(() -> {
      Thread.sleep(100);
    });

    final RecordingTaskManager<NoMetrics> unit = new RecordingTaskManager<>(id, subscribers,
        executor, controller, workerBodyFactory, queue, topic);

    unit.run();

    // Assert
    assertThat(unit.getEventsAndActions()).isEqualTo(List.of(
        EventOrAction.of(TaskAction.newStartWorkerTaskAction()),
        EventOrAction.of(new WorkerStartedEvent(0)), EventOrAction.of(new WorkerCompletedEvent(0)),
        EventOrAction.of(TaskAction.newSucceedTask())));
  }

  @Test
  @Timeout(5)
  void givenSuccessfulWorkerWithTwoConcurrency_whenRun_thenGetExpectedEventsAndActions()
      throws Exception {
    final TaskController controller = new TestTaskController(2);

    final AtomicInteger counter = new AtomicInteger(0);
    final WorkerBodyFactory<NoMetrics> workerBodyFactory = newWorkerBodyFactory(() -> {
      final int count = counter.getAndIncrement();
      return () -> {
        Thread.sleep(10 * count);
      };
    });

    final RecordingTaskManager<NoMetrics> unit = new RecordingTaskManager<>(id, subscribers,
        executor, controller, workerBodyFactory, queue, topic);

    unit.run();

    // Assert
    assertThat(unit.getEventsAndActions()).containsExactlyInAnyOrder(
        EventOrAction.of(TaskAction.newStartWorkerTaskAction()),
        EventOrAction.of(TaskAction.newStartWorkerTaskAction()),
        EventOrAction.of(new WorkerStartedEvent(0)), EventOrAction.of(new WorkerStartedEvent(1)),
        EventOrAction.of(new WorkerCompletedEvent(0)),
        EventOrAction.of(new WorkerCompletedEvent(1)),
        EventOrAction.of(TaskAction.newSucceedTask()));
  }

  @Test
  @Timeout(5)
  void givenNoWorkers_whenRunAndInterrupt_thenGetExpectedEventsAndActions() throws Exception {
    // We should cancel properly if there are no workers running!

    final TaskController controller = new TestTaskController(0);

    final WorkerBodyFactory<NoMetrics> workerBodyFactory = newWorkerBodyFactory(() -> {
      throw new UnsupportedOperationException("no workers!");
    });

    final RecordingTaskManager<NoMetrics> unit = new RecordingTaskManager<>(id, subscribers,
        executor, controller, workerBodyFactory, queue, topic);

    final Thread manager = new Thread(() -> {
      try {
        unit.run();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    manager.start();

    manager.interrupt();

    manager.join();

    // Assert
    assertThat(unit.getEventsAndActions())
        .isEqualTo(List.of(EventOrAction.of(TaskAction.newCancelTask())));
  }

  @Test
  @Timeout(5)
  void givenWaitingWorkerWithOneConcurrency_whenRunAndInterrupt_thenGetExpectedEventsAndActions()
      throws Exception {
    final TaskController controller = new TestTaskController(1);

    final CountDownLatch working = new CountDownLatch(1);
    final WorkerBodyFactory<NoMetrics> workerBodyFactory = newWorkerBodyFactory(() -> {
      working.countDown();
      Thread.sleep(100000);
    });

    final RecordingTaskManager<NoMetrics> unit = new RecordingTaskManager<>(id, subscribers,
        executor, controller, workerBodyFactory, queue, topic);

    final Thread manager = new Thread(() -> {
      try {
        unit.run();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    manager.start();

    working.await();

    manager.interrupt();

    manager.join();

    // Assert
    assertThat(unit.getEventsAndActions()).isEqualTo(List.of(
        EventOrAction.of(TaskAction.newStartWorkerTaskAction()),
        EventOrAction.of(new WorkerStartedEvent(0)),
        EventOrAction.of(TaskAction.newStopWorkerTaskAction()),
        EventOrAction.of(new WorkerStoppedEvent(0)), EventOrAction.of(TaskAction.newCancelTask())));
  }

  @Test
  @Timeout(50000)
  void givenWaitingWorkerWithTwoConcurrency_whenRunAndInterrupt_thenGetExpectedEventsAndActions()
      throws Exception {
    final TaskController controller = new TestTaskController(2);

    final AtomicInteger counter = new AtomicInteger(0);
    final CountDownLatch working = new CountDownLatch(2);
    final WorkerBodyFactory<NoMetrics> workerBodyFactory = newWorkerBodyFactory(() -> {
      final int count = counter.getAndIncrement();
      return () -> {
        working.countDown();
        try {
          Thread.sleep(100000);
        } catch (InterruptedException e) {
          // Ignore
          Thread.sleep(10 * count);
          throw new InterruptedException();
        }
      };
    });

    final RecordingTaskManager<NoMetrics> unit = new RecordingTaskManager<>(id, subscribers,
        executor, controller, workerBodyFactory, queue, topic);

    final Thread manager = new Thread(() -> {
      try {
        unit.run();
      } catch (InterruptedException e) {
        // This is expected
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    manager.start();

    working.await();

    manager.interrupt();

    manager.join();

    // Assert
    assertThat(unit.getEventsAndActions()).containsExactlyInAnyOrder(
        EventOrAction.of(TaskAction.newStartWorkerTaskAction()),
        EventOrAction.of(TaskAction.newStartWorkerTaskAction()),
        EventOrAction.of(new WorkerStartedEvent(0)), EventOrAction.of(new WorkerStartedEvent(1)),
        EventOrAction.of(TaskAction.newStopWorkerTaskAction()),
        EventOrAction.of(TaskAction.newStopWorkerTaskAction()),
        EventOrAction.of(new WorkerStoppedEvent(0)), EventOrAction.of(new WorkerStoppedEvent(1)),
        EventOrAction.of(TaskAction.newCancelTask()));
  }

  @Test
  @Timeout(500)
  void givenFailingWorkerWithOneConcurrency_whenRun_thenGetExpectedEventsAndActions()
      throws Exception {
    final TaskController controller = new TestTaskController(1);

    final WorkerBodyFactory<NoMetrics> workerBodyFactory = newWorkerBodyFactory(() -> {
      return () -> {
        throw new RuntimeException("simulated failure");
      };
    });

    final RecordingTaskManager<NoMetrics> unit = new RecordingTaskManager<>(id, subscribers,
        executor, controller, workerBodyFactory, queue, topic);

    unit.run();

    // This is funky. We have to extract the cause from the event and action so we can
    // (conveniently) assert that they are the same. If these don't succeed, then the
    // test will fail, which is just fine.
    final Throwable cause1 =
        ((WorkerFailedEvent) unit.getEventsAndActions().get(2).event()).cause();
    final ExecutionException cause2 =
        ((FailTaskAction) unit.getEventsAndActions().get(3).action()).cause();

    // Assert
    assertThat(unit.getEventsAndActions())
        .isEqualTo(List.of(EventOrAction.of(TaskAction.newStartWorkerTaskAction()),
            EventOrAction.of(new WorkerStartedEvent(0)),
            EventOrAction.of(new WorkerFailedEvent(0, cause1)),
            EventOrAction.of(TaskAction.newFailTask(cause2))));
  }

  @Test
  @Timeout(500)
  void givenFailingWorkerWithTwoConcurrency_whenRun_thenGetExpectedEventsAndActions()
      throws Exception {
    final TaskController controller = new TestTaskController(2);

    final AtomicInteger counter = new AtomicInteger(0);
    final WorkerBodyFactory<NoMetrics> workerBodyFactory = newWorkerBodyFactory(() -> {
      final int count = counter.getAndIncrement();
      return () -> {
        if (count == 0) {
          // Sleep for a moment, then complete
          Thread.sleep(10);
        } else if (count == 1) {
          // Sleep for a (longer) moment, then fail
          Thread.sleep(20);
          throw new RuntimeException("simulated failure");
        }
      };
    });

    final RecordingTaskManager<NoMetrics> unit = new RecordingTaskManager<>(id, subscribers,
        executor, controller, workerBodyFactory, queue, topic);

    unit.run();

    // This is funky. We have to extract the cause from the event and action so we can
    // (conveniently) assert that they are the same. If these don't succeed, then the
    // test will fail, which is just fine.
    final Throwable cause1 =
        ((WorkerFailedEvent) unit.getEventsAndActions().get(5).event()).cause();
    final ExecutionException cause2 =
        ((FailTaskAction) unit.getEventsAndActions().get(6).action()).cause();

    // Assert
    assertThat(unit.getEventsAndActions()).containsExactlyInAnyOrder(
        EventOrAction.of(TaskAction.newStartWorkerTaskAction()),
        EventOrAction.of(TaskAction.newStartWorkerTaskAction()),
        EventOrAction.of(new WorkerStartedEvent(0)), EventOrAction.of(new WorkerStartedEvent(1)),
        EventOrAction.of(new WorkerCompletedEvent(0)),
        EventOrAction.of(new WorkerFailedEvent(1, cause1)),
        EventOrAction.of(TaskAction.newFailTask(cause2)));
  }

  @Test
  void givenAllStates_whenAllPossibleTransitions_thenValidOnesSucceedAndInvalidOnesFail() {
    // Test valid transitions
    assertThat(TaskState.READY.to(TaskState.RUNNING)).isEqualTo(TaskState.RUNNING);
    assertThat(TaskState.RUNNING.to(TaskState.SUCCEEDED)).isEqualTo(TaskState.SUCCEEDED);
    assertThat(TaskState.RUNNING.to(TaskState.CANCELED)).isEqualTo(TaskState.CANCELED);
    assertThat(TaskState.RUNNING.to(TaskState.FAILED)).isEqualTo(TaskState.FAILED);

    // Test invalid transitions
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> TaskState.READY.to(TaskState.SUCCEEDED));
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> TaskState.READY.to(TaskState.CANCELED));
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> TaskState.READY.to(TaskState.FAILED));

    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> TaskState.RUNNING.to(TaskState.READY));

    // Test terminal states - they should all throw exceptions for any transition
    for (TaskState source : new TaskState[] {TaskState.SUCCEEDED, TaskState.CANCELED,
        TaskState.FAILED}) {
      for (TaskState target : TaskState.values()) {
        if (source != target) {
          assertThatExceptionOfType(IllegalStateException.class)
              .isThrownBy(() -> source.to(target));
        }
      }
    }
  }
}
