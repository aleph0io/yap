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
package io.aleph0.yap.core.pipeline;

import static java.util.Collections.unmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import io.aleph0.yap.core.pipeline.PipelineManager;
import io.aleph0.yap.core.pipeline.PipelineManager.PipelinePhase;
import io.aleph0.yap.core.pipeline.PipelineManager.PipelineState;
import io.aleph0.yap.core.pipeline.PipelineManager.TaskCancelledEvent;
import io.aleph0.yap.core.pipeline.PipelineManager.TaskStartedEvent;
import io.aleph0.yap.core.pipeline.action.CancelPipelineAction;
import io.aleph0.yap.core.pipeline.action.CancelTaskPipelineAction;
import io.aleph0.yap.core.pipeline.action.FailPipelineAction;
import io.aleph0.yap.core.pipeline.action.PipelineAction;
import io.aleph0.yap.core.pipeline.action.StartTaskPipelineAction;
import io.aleph0.yap.core.pipeline.action.SucceedPipelineAction;
import io.aleph0.yap.core.task.TaskManager;
import io.aleph0.yap.core.task.TestTaskController;
import io.aleph0.yap.core.task.TaskManager.WorkerBody;
import io.aleph0.yap.core.task.TaskManager.WorkerBodyFactory;
import io.aleph0.yap.core.transport.Channel;
import io.aleph0.yap.core.transport.Queue;
import io.aleph0.yap.core.transport.Topic;
import io.aleph0.yap.core.transport.channel.DefaultChannel;
import io.aleph0.yap.core.transport.queue.DefaultQueue;
import io.aleph0.yap.core.transport.topic.DefaultTopic;
import io.aleph0.yap.core.util.NoMetrics;

public class PipelineManagerTest {
  private final int id = 1;
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

  public static record EventOrAction(PipelineManager.PipelineEvent event, PipelineAction action) {
    public static EventOrAction of(PipelineManager.PipelineEvent event) {
      return new EventOrAction(event, null);
    }

    public static EventOrAction of(PipelineAction action) {
      return new EventOrAction(null, action);
    }
  }

  public static class RecordingPipelineManager extends PipelineManager {
    public RecordingPipelineManager(int id, ExecutorService executor, PipelineController controller,
        List<TaskManager<?>> tasks) {
      super(id, executor, controller, tasks);
    }

    private final List<EventOrAction> eoas = new ArrayList<>();

    @Override
    List<PipelineAction> handleTaskEvent(PipelineEvent event) {
      eoas.add(EventOrAction.of(event));
      return super.handleTaskEvent(event);
    }

    @Override
    void performPipelineAction(PipelineAction action) throws InterruptedException {
      eoas.add(EventOrAction.of(action));
      super.performPipelineAction(action);
    }

    public List<EventOrAction> getEventsAndActions() {
      return unmodifiableList(eoas);
    }
  }

  private WorkerBodyFactory<NoMetrics> createMockWorkerBodyFactory(long sleepTime,
      boolean shouldFail) {
    return new WorkerBodyFactory<>() {
      @Override
      public WorkerBody newWorkerBody() {
        return () -> {
          Thread.sleep(sleepTime);
          if (shouldFail) {
            throw new RuntimeException("simulated failure");
          }
        };
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

  private TaskManager<NoMetrics> createMockTask(String id, long sleepTime, boolean shouldFail) {
    final Channel<String> channel = new DefaultChannel<>();
    final Queue<String> queue = new DefaultQueue<>(10, List.of(channel));
    final Topic<String> topic = new DefaultTopic<>(List.of(channel));
    return new TaskManager<>(id, Set.of("subscriber"), executor, new TestTaskController(1),
        createMockWorkerBodyFactory(sleepTime, shouldFail), queue, topic);
  }

  @Test
  @Timeout(5)
  void givenSuccessfulTasks_whenRun_thenPipelineCompletesSuccessfully() throws Exception {
    // Given
    List<TaskManager<?>> tasks = List.of(createMockTask("task1", 100, false),
        createMockTask("task2", 200, false), createMockTask("task3", 300, false));

    PipelineController controller = new TestPipelineController(
        Map.of("task1", Set.of("task2"), "task2", Set.of("task3"), "task3", Set.of()));

    RecordingPipelineManager unit = new RecordingPipelineManager(id, executor, controller, tasks);

    // When
    unit.run();

    // Then
    assertThat(unit.getEventsAndActions()).contains(
        EventOrAction.of(new StartTaskPipelineAction("task1")),
        EventOrAction.of(new StartTaskPipelineAction("task2")),
        EventOrAction.of(new StartTaskPipelineAction("task3")),
        EventOrAction.of(new SucceedPipelineAction()));
  }

  @Test
  @Timeout(5)
  void givenFailingTask_whenRun_thenPipelineFails() throws Exception {
    // Given
    List<TaskManager<?>> tasks = List.of(
        // This task will succeed
        createMockTask("task1", 100, false),
        // This task will fail
        createMockTask("task2", 200, true),
        // This task will succeed
        createMockTask("task3", 300, false));

    PipelineController controller = new TestPipelineController(
        Map.of("task1", Set.of("task2"), "task2", Set.of("task3"), "task3", Set.of()));

    RecordingPipelineManager unit = new RecordingPipelineManager(id, executor, controller, tasks);

    // When/Then
    try {
      unit.run();
    } catch (Exception e) {
      // Expected exception
    }

    // Then
    List<EventOrAction> eoas = unit.getEventsAndActions();
    assertThat(eoas).anySatisfy(eoa -> {
      if (eoa.action() != null && eoa.action() instanceof CancelTaskPipelineAction) {
        // Verify cancel actions for other tasks
        CancelTaskPipelineAction cancelAction = (CancelTaskPipelineAction) eoa.action();
        assertThat(cancelAction.id()).isIn("task1", "task3");
      }
    });

    // Check that we have a fail action
    assertThat(eoas)
        .anySatisfy(eoa -> assertThat(eoa.action()).isInstanceOf(FailPipelineAction.class));
  }

  @Test
  @Timeout(5)
  void givenRunningPipeline_whenInterrupted_thenCancelsAllTasks() throws Exception {
    // Given
    List<TaskManager<?>> tasks = List.of(createMockTask("task1", 100, false),
        createMockTask("task2", 200, false), createMockTask("task3", 300, false));

    PipelineController controller = new TestPipelineController(
        Map.of("task1", Set.of("task2"), "task2", Set.of("task3"), "task3", Set.of()));

    CountDownLatch taskStartedLatch = new CountDownLatch(3);
    RecordingPipelineManager unit = new RecordingPipelineManager(id, executor, controller, tasks);
    unit.addLifecycleListener(new PipelineManager.LifecycleListener() {
      @Override
      public void onPipelineTaskWorkerStarted(int pipeline, String task, int worker) {
        taskStartedLatch.countDown();
      }
    });

    // When
    Thread pipelineThread = new Thread(() -> {
      try {
        unit.run();
      } catch (Exception e) {
        // Expected
      }
    });

    pipelineThread.start();

    // Wait for all tasks to start
    taskStartedLatch.await();

    // Then interrupt the pipeline
    pipelineThread.interrupt();
    pipelineThread.join();

    // Then
    assertThat(unit.getEventsAndActions()).contains(
        EventOrAction.of(new StartTaskPipelineAction("task3")),
        EventOrAction.of(new TaskStartedEvent("task3")),
        EventOrAction.of(new StartTaskPipelineAction("task2")),
        EventOrAction.of(new TaskStartedEvent("task2")),
        EventOrAction.of(new StartTaskPipelineAction("task1")),
        EventOrAction.of(new TaskStartedEvent("task1")),
        EventOrAction.of(new CancelTaskPipelineAction("task1")),
        EventOrAction.of(new TaskCancelledEvent("task1")),
        EventOrAction.of(new CancelTaskPipelineAction("task2")),
        EventOrAction.of(new TaskCancelledEvent("task2")),
        EventOrAction.of(new CancelTaskPipelineAction("task3")),
        EventOrAction.of(new TaskCancelledEvent("task3")),
        EventOrAction.of(new CancelPipelineAction()));
  }

  @Test
  @Timeout(5)
  void givenTaskWithLifecycleEvents_whenRun_thenPipelineReceivesAndNotifiesEvents()
      throws Exception {
    // Given
    AtomicInteger workerStartCount = new AtomicInteger(0);
    AtomicInteger workerCompletedCount = new AtomicInteger(0);

    List<TaskManager<?>> tasks = List.of(createMockTask("task", 100, false));

    PipelineController controller = new TestPipelineController(Map.of("task", Set.of()));

    RecordingPipelineManager unit = new RecordingPipelineManager(id, executor, controller, tasks);

    // Add a lifecycle listener to count events
    unit.addLifecycleListener(new PipelineManager.LifecycleListener() {
      @Override
      public void onPipelineTaskWorkerStarted(int pipeline, String task, int worker) {
        workerStartCount.incrementAndGet();
      }

      @Override
      public void onPipelineTaskWorkerCompletedNormally(int pipeline, String task, int worker) {
        workerCompletedCount.incrementAndGet();
      }
    });

    // When
    unit.run();

    // Then
    assertThat(workerStartCount.get()).isEqualTo(1);
    assertThat(workerCompletedCount.get()).isEqualTo(1);
  }

  @Test
  @Timeout(5)
  void givenMetricsCheck_whenRun_thenCanRetrieveMetrics() throws Exception {
    // Given
    List<TaskManager<?>> tasks =
        List.of(createMockTask("task1", 100, false), createMockTask("task2", 100, false));

    PipelineController controller = new TestPipelineController(
        Map.of("task1", Set.of("task2"), "task2", Set.of()), Duration.ofMillis(100));

    RecordingPipelineManager unit = new RecordingPipelineManager(id, executor, controller, tasks);

    // When
    Thread pipelineThread = new Thread(() -> {
      try {
        unit.run();
      } catch (Exception e) {
        // Expected
      }
    });
    pipelineThread.start();

    // Check metrics during execution
    Thread.sleep(100);
    PipelineManager.Metrics metrics = unit.checkMetrics();

    // Then cancel to finish the test
    pipelineThread.interrupt();
    pipelineThread.join();

    // Then
    assertThat(metrics).isNotNull();
    assertThat(metrics.id()).isEqualTo(id);
    assertThat(metrics.phase()).isEqualTo(PipelinePhase.RUNNING);
    assertThat(metrics.tasks()).hasSize(2);
    assertThat(metrics.tasks()).containsKeys("task1", "task2");
  }

  @Test
  void givenAllPipelineStates_whenAllPossibleTransitions_thenValidOnesSucceedAndInvalidOnesFail() {
    // Test valid transitions
    assertThat(PipelineState.READY.to(PipelineState.RUNNING)).isEqualTo(PipelineState.RUNNING);
    assertThat(PipelineState.RUNNING.to(PipelineState.COMPLETED))
        .isEqualTo(PipelineState.COMPLETED);
    assertThat(PipelineState.RUNNING.to(PipelineState.CANCELLED))
        .isEqualTo(PipelineState.CANCELLED);
    assertThat(PipelineState.RUNNING.to(PipelineState.FAILED)).isEqualTo(PipelineState.FAILED);

    // Test invalid transitions
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> PipelineState.READY.to(PipelineState.COMPLETED));
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> PipelineState.READY.to(PipelineState.CANCELLED));
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> PipelineState.READY.to(PipelineState.FAILED));

    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> PipelineState.RUNNING.to(PipelineState.READY));

    // Test terminal states - they should all throw exceptions for any transition
    for (PipelineState source : new PipelineState[] {PipelineState.COMPLETED,
        PipelineState.CANCELLED, PipelineState.FAILED}) {
      for (PipelineState target : PipelineState.values()) {
        if (source != target) {
          assertThatExceptionOfType(IllegalStateException.class)
              .isThrownBy(() -> source.to(target));
        }
      }
    }
  }
}
