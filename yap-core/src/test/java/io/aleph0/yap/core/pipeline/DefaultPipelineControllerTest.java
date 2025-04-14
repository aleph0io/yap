package io.aleph0.yap.core.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import io.aleph0.yap.core.pipeline.DefaultPipelineController;
import io.aleph0.yap.core.pipeline.DefaultPipelineController.PipelineState;
import io.aleph0.yap.core.pipeline.action.CancelPipelineAction;
import io.aleph0.yap.core.pipeline.action.CancelTaskPipelineAction;
import io.aleph0.yap.core.pipeline.action.FailPipelineAction;
import io.aleph0.yap.core.pipeline.action.PipelineAction;
import io.aleph0.yap.core.pipeline.action.StartTaskPipelineAction;
import io.aleph0.yap.core.pipeline.action.SucceedPipelineAction;

class DefaultPipelineControllerTest {
  private DefaultPipelineController controller;
  private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(5);
  private Map<String, Set<String>> graph;

  @BeforeEach
  void setUp() {
    graph = new HashMap<>();
    // Simple linear pipeline: task1 -> task2 -> task3
    graph.put("task1", Set.of("task2"));
    graph.put("task2", Set.of("task3"));
    graph.put("task3", Set.of());

    controller = new DefaultPipelineController(graph, HEARTBEAT_INTERVAL);
  }

  @Test
  void givenReadyState_whenPipelineStarted_thenStartsSourceTasks() {
    // Given a controller in READY state

    // When pipeline is started
    List<PipelineAction> actions = controller.onPipelineStarted();

    // Then it should start the source task (task1)
    assertThat(actions).hasSize(1);
    assertThat(actions.get(0)).isInstanceOf(StartTaskPipelineAction.class);
    assertThat(((StartTaskPipelineAction) actions.get(0)).id()).isEqualTo("task3");
  }

  @Test
  void givenRunningState_whenTaskStarts_thenStartsDownstreamTasks() {
    // Given a controller where pipeline has started
    List<PipelineAction> actions1 = controller.onPipelineStarted();
    assertThat(actions1).isEqualTo(List.of(new StartTaskPipelineAction("task3")));

    // When task1 starts
    List<PipelineAction> actions2 = controller.onTaskStarted("task3");

    // Then it should start its downstream task (task2)
    assertThat(actions2).isEqualTo(List.of(new StartTaskPipelineAction("task2")));
  }

  @Test
  void givenRunningState_whenAllTasksStartAndCompleteInCorrectOrder_thenTransitionsToCompleted() {
    // Given a pipeline with all tasks started... Note that downstream tasks are started first
    List<PipelineAction> actions1 = controller.onPipelineStarted();
    assertThat(actions1).isEqualTo(List.of(new StartTaskPipelineAction("task3")));

    List<PipelineAction> actions2 = controller.onTaskStarted("task3");
    assertThat(actions2).isEqualTo(List.of(new StartTaskPipelineAction("task2")));

    List<PipelineAction> actions3 = controller.onTaskStarted("task2");
    assertThat(actions3).isEqualTo(List.of(new StartTaskPipelineAction("task1")));

    List<PipelineAction> actions4 = controller.onTaskStarted("task1");
    assertThat(actions4).isEmpty();

    // When tasks complete in reverse order... Note that task1 is the first to complete
    List<PipelineAction> actions5 = controller.onTaskCompleted("task1");
    assertThat(actions5).isEmpty();

    List<PipelineAction> actions6 = controller.onTaskCompleted("task2");
    assertThat(actions6).isEmpty();

    List<PipelineAction> actions7 = controller.onTaskCompleted("task3");

    // Then it should transition to COMPLETED and succeed the pipeline
    assertThat(actions7).isEqualTo(List.of(new SucceedPipelineAction()));
    assertThat(controller.state).isEqualTo(PipelineState.COMPLETED);
  }

  @Test
  void givenRunningState_whenCancelRequested_thenCancelsAllRunningTasks() {
    // Given a pipeline with all tasks started
    List<PipelineAction> actions1 = controller.onPipelineStarted();
    assertThat(actions1).isEqualTo(List.of(new StartTaskPipelineAction("task3")));

    List<PipelineAction> actions2 = controller.onTaskStarted("task3");
    assertThat(actions2).isEqualTo(List.of(new StartTaskPipelineAction("task2")));

    List<PipelineAction> actions3 = controller.onTaskStarted("task2");
    assertThat(actions3).isEqualTo(List.of(new StartTaskPipelineAction("task1")));

    List<PipelineAction> actions4 = controller.onTaskStarted("task1");
    assertThat(actions4).isEmpty();

    // When cancel is requested
    List<PipelineAction> actions5 = controller.onCancelRequested();

    // Then it should cancel all running tasks and transition to CANCELING
    assertThat(actions5).containsExactlyInAnyOrder(PipelineAction.cancelTask("task1"),
        PipelineAction.cancelTask("task2"), PipelineAction.cancelTask("task3"));
    assertThat(controller.state).isEqualTo(PipelineState.CANCELING);
  }

  @Test
  void givenCancelingState_whenAllTasksCancel_thenTransitionsToCanceled() {
    // Given a canceling pipeline
    List<PipelineAction> actions1 = controller.onPipelineStarted();
    assertThat(actions1).isEqualTo(List.of(new StartTaskPipelineAction("task3")));

    List<PipelineAction> actions2 = controller.onTaskStarted("task3");
    assertThat(actions2).isEqualTo(List.of(new StartTaskPipelineAction("task2")));

    List<PipelineAction> actions3 = controller.onTaskStarted("task2");
    assertThat(actions3).isEqualTo(List.of(new StartTaskPipelineAction("task1")));

    List<PipelineAction> actions4 = controller.onTaskStarted("task1");
    assertThat(actions4).isEmpty();

    List<PipelineAction> actions5 = controller.onCancelRequested();
    assertThat(actions5).containsExactlyInAnyOrder(PipelineAction.cancelTask("task1"),
        PipelineAction.cancelTask("task2"), PipelineAction.cancelTask("task3"));

    // When all tasks cancel
    List<PipelineAction> actions6 = controller.onTaskCancelled("task3");
    assertThat(actions6).isEmpty();

    List<PipelineAction> actions7 = controller.onTaskCancelled("task2");
    assertThat(actions7).isEmpty();

    List<PipelineAction> actions8 = controller.onTaskCancelled("task1");

    // Then it should transition to CANCELED with a cancel action
    assertThat(actions8).isEqualTo(List.of(new CancelPipelineAction()));
    assertThat(controller.state).isEqualTo(PipelineState.CANCELED);
  }

  @Test
  void givenRunningState_whenTaskFails_thenCancelsRemainingTasksAndTransitionsToFailing() {
    // Given a running pipeline
    List<PipelineAction> actions1 = controller.onPipelineStarted();
    assertThat(actions1).isEqualTo(List.of(new StartTaskPipelineAction("task3")));

    List<PipelineAction> actions2 = controller.onTaskStarted("task3");
    assertThat(actions2).isEqualTo(List.of(new StartTaskPipelineAction("task2")));

    List<PipelineAction> actions3 = controller.onTaskStarted("task2");
    assertThat(actions3).isEqualTo(List.of(new StartTaskPipelineAction("task1")));

    List<PipelineAction> actions4 = controller.onTaskStarted("task1");
    assertThat(actions4).isEmpty();

    // When a task fails
    RuntimeException exception = new RuntimeException("Test exception");
    List<PipelineAction> actions5 = controller.onTaskFailed("task2", exception);

    // Then it should transition to FAILING and cancel all other running tasks
    assertThat(controller.state).isEqualTo(PipelineState.FAILING);
    assertThat(actions5).hasSize(2);
    assertThat(actions5).containsExactlyInAnyOrder(new CancelTaskPipelineAction("task1"),
        new CancelTaskPipelineAction("task3"));
  }

  @Test
  void givenFailingState_whenAllRemainingTasksCancel_thenTransitionsToFailed() {
    // Given a failing pipeline
    List<PipelineAction> actions1 = controller.onPipelineStarted();
    assertThat(actions1).isEqualTo(List.of(new StartTaskPipelineAction("task3")));

    List<PipelineAction> actions2 = controller.onTaskStarted("task3");
    assertThat(actions2).isEqualTo(List.of(new StartTaskPipelineAction("task2")));

    List<PipelineAction> actions3 = controller.onTaskStarted("task2");
    assertThat(actions3).isEqualTo(List.of(new StartTaskPipelineAction("task1")));

    List<PipelineAction> actions4 = controller.onTaskStarted("task1");
    assertThat(actions4).isEmpty();

    RuntimeException exception = new RuntimeException("Test exception");
    List<PipelineAction> actions5 = controller.onTaskFailed("task2", exception);
    assertThat(actions5).containsExactlyInAnyOrder(new CancelTaskPipelineAction("task1"),
        new CancelTaskPipelineAction("task3"));

    // When all remaining tasks cancel
    List<PipelineAction> actions6 = controller.onTaskCancelled("task3");
    assertThat(actions6).isEmpty();

    List<PipelineAction> actions7 = controller.onTaskCancelled("task1");

    // Then it should transition to FAILED with a fail action
    assertThat(controller.state).isEqualTo(PipelineState.FAILED);
    assertThat(actions7).hasSize(1);
    assertThat(actions7.get(0)).isInstanceOf(FailPipelineAction.class);
    assertThat(((FailPipelineAction) actions7.get(0)).cause())
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  void givenRunningState_whenParentTaskCompletesBeforeChild_thenTransitionsToFailing() {
    // Given a pipeline with all tasks started
    startStandardPipelineTasks();

    // When a downstream task completes before its parent
    List<PipelineAction> actions = controller.onTaskCompleted("task2");

    // Then it should fail and cancel remaining tasks
    assertThat(controller.state).isEqualTo(PipelineState.FAILING);
    assertThat(actions).containsExactlyInAnyOrder(new CancelTaskPipelineAction("task1"),
        new CancelTaskPipelineAction("task3"));
  }

  @Test
  void givenRunningState_whenHeartbeatOccurs_thenNoActionsTaken() {
    // Given a running pipeline
    startStandardPipelineTasks();

    // When heartbeat occurs
    List<PipelineAction> actions = controller.onHeartbeat();

    // Then no actions are returned
    assertThat(actions).isEmpty();
  }

  @Test
  void givenCancelingState_whenCancelRequestedAgain_thenNoActionsReturned() {
    // Given a canceling pipeline
    startStandardPipelineTasks();

    // When cancel requested
    List<PipelineAction> actions1 = controller.onCancelRequested();
    assertThat(actions1).containsExactlyInAnyOrder(PipelineAction.cancelTask("task1"),
        PipelineAction.cancelTask("task2"), PipelineAction.cancelTask("task3"));

    // When cancel requested again
    List<PipelineAction> actions2 = controller.onCancelRequested();

    // Then no additional actions are taken
    assertThat(actions2).isEmpty();
  }

  @Test
  void givenRunningState_whenTaskCancelsUnexpectedly_thenFailsPipeline() {
    // Given a running pipeline
    startStandardPipelineTasks();

    // When a task cancels unexpectedly (without a cancel request)
    List<PipelineAction> actions = controller.onTaskCancelled("task2");

    // Then it should transition to FAILING and cancel remaining tasks
    assertThat(controller.state).isEqualTo(PipelineState.FAILING);
    assertThat(actions).containsExactlyInAnyOrder(new CancelTaskPipelineAction("task1"),
        new CancelTaskPipelineAction("task3"));
  }

  @Test
  @Disabled("""
      We do not support cyclic pipelines, but we do not check for cycles in the
      DefaultPipelineController since we already check for cycles in the PipelineBuilder.
      """)
  void givenCyclicPipeline_whenTasksStarted_thenIllegalArgumentException() {}

  @Test
  void givenTaskAlreadyFailed_whenAnotherTaskFails_thenCaptureBothExceptions() {
    // Given a failing pipeline with one failed task
    startStandardPipelineTasks();

    RuntimeException exception1 = new RuntimeException("First exception");
    List<PipelineAction> actions1 = controller.onTaskFailed("task2", exception1);
    assertThat(actions1).containsExactlyInAnyOrder(new CancelTaskPipelineAction("task1"),
        new CancelTaskPipelineAction("task3"));
    assertThat(controller.state).isEqualTo(PipelineState.FAILING);

    // When another task fails
    RuntimeException exception2 = new RuntimeException("Second exception");
    List<PipelineAction> actions2 = controller.onTaskFailed("task3", exception2);
    assertThat(actions2).isEmpty();
    assertThat(controller.state).isEqualTo(PipelineState.FAILING);

    // Complete the last task to check the final failure
    List<PipelineAction> actions3 = controller.onTaskCancelled("task1");
    assertThat(actions3).hasSize(1);
    assertThat(actions3.get(0)).isInstanceOf(FailPipelineAction.class);

    // Ensure both exceptions are captured (first as primary, second as suppressed)
    ExecutionException failureReason =
        (ExecutionException) ((FailPipelineAction) actions3.get(0)).cause();
    assertThat(failureReason).isNotNull();
    assertThat(failureReason.getCause()).isNotNull();
    assertThat(failureReason.getSuppressed()).hasSize(1);
  }

  private void startStandardPipelineTasks() {
    List<PipelineAction> actions1 = controller.onPipelineStarted();
    assertThat(actions1).isEqualTo(List.of(new StartTaskPipelineAction("task3")));

    List<PipelineAction> actions2 = controller.onTaskStarted("task3");
    assertThat(actions2).isEqualTo(List.of(new StartTaskPipelineAction("task2")));

    List<PipelineAction> actions3 = controller.onTaskStarted("task2");
    assertThat(actions3).isEqualTo(List.of(new StartTaskPipelineAction("task1")));

    List<PipelineAction> actions4 = controller.onTaskStarted("task1");
    assertThat(actions4).isEmpty();
  }

  // CUSTOM PIPELINE TOPOLOGIES ////////////////////////////////////////////////////////////////////

  @Test
  void givenBranchingPipeline_whenAllTasksCompleteInValidOrder_thenTransitionsToCompleted() {
    // Given a branching pipeline structure: task1 -> task2 -> task3
    // -> task4
    Map<String, Set<String>> branchingGraph = new HashMap<>();
    branchingGraph.put("task1", Set.of("task2", "task4"));
    branchingGraph.put("task2", Set.of("task3"));
    branchingGraph.put("task3", Set.of());
    branchingGraph.put("task4", Set.of());

    controller = new DefaultPipelineController(branchingGraph, HEARTBEAT_INTERVAL);

    List<PipelineAction> actions1 = controller.onPipelineStarted();
    assertThat(actions1).containsExactlyInAnyOrder(new StartTaskPipelineAction("task3"),
        new StartTaskPipelineAction("task4"));

    List<PipelineAction> actions2 = controller.onTaskStarted("task4");
    assertThat(actions2).isEmpty();

    List<PipelineAction> actions3 = controller.onTaskStarted("task3");
    assertThat(actions3).isEqualTo(List.of(new StartTaskPipelineAction("task2")));

    List<PipelineAction> actions4 = controller.onTaskStarted("task2");
    assertThat(actions4).isEqualTo(List.of(new StartTaskPipelineAction("task1")));

    // When all tasks complete in an appropriate order (leaves first, then branches, then root)
    List<PipelineAction> actions5 = controller.onTaskCompleted("task1");
    assertThat(actions5).isEmpty();

    List<PipelineAction> actions6 = controller.onTaskCompleted("task2");
    assertThat(actions6).isEmpty();

    List<PipelineAction> actions7 = controller.onTaskCompleted("task4");
    assertThat(actions7).isEmpty();

    // When the root task completes last
    List<PipelineAction> actions8 = controller.onTaskCompleted("task3");

    // Then it should transition to COMPLETED
    assertThat(controller.state).isEqualTo(PipelineState.COMPLETED);
    assertThat(actions8).isEqualTo(List.of(new SucceedPipelineAction()));
  }

  @Test
  void givenMergingPipeline_whenAllTasksCompleteInValidOrder_thenTransitionsToCompleted() {
    // Given a merging pipeline structure: task1 -> task3
    // task2 ->
    Map<String, Set<String>> mergingGraph = new HashMap<>();
    mergingGraph.put("task1", Set.of("task3"));
    mergingGraph.put("task2", Set.of("task3"));
    mergingGraph.put("task3", Set.of());

    controller = new DefaultPipelineController(mergingGraph, HEARTBEAT_INTERVAL);
    List<PipelineAction> actions1 = controller.onPipelineStarted();
    assertThat(actions1).isEqualTo(List.of(new StartTaskPipelineAction("task3")));

    List<PipelineAction> actions2 = controller.onTaskStarted("task3");
    assertThat(actions2).containsExactlyInAnyOrder(new StartTaskPipelineAction("task1"),
        new StartTaskPipelineAction("task2"));

    List<PipelineAction> actions3 = controller.onTaskStarted("task1");
    assertThat(actions3).isEmpty();

    List<PipelineAction> actions4 = controller.onTaskStarted("task2");
    assertThat(actions4).isEmpty();

    // When all tasks complete in appropriate order (leaf first, then parents)
    List<PipelineAction> actions5 = controller.onTaskCompleted("task1");
    assertThat(actions5).isEmpty();

    List<PipelineAction> actions6 = controller.onTaskCompleted("task2");
    assertThat(actions6).isEmpty();

    List<PipelineAction> actions7 = controller.onTaskCompleted("task3");

    // Then it should transition to COMPLETED
    assertThat(controller.state).isEqualTo(PipelineState.COMPLETED);
    assertThat(actions7).isEqualTo(List.of(new SucceedPipelineAction()));
  }
}
