package io.aleph0.yap.core.build;

import static java.util.stream.Collectors.toMap;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import io.aleph0.yap.core.ConsumerWorker;
import io.aleph0.yap.core.Pipeline;
import io.aleph0.yap.core.ProcessorWorker;
import io.aleph0.yap.core.ProducerWorker;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.core.pipeline.DefaultPipeline;
import io.aleph0.yap.core.pipeline.DefaultPipelineController;
import io.aleph0.yap.core.pipeline.PipelineController;
import io.aleph0.yap.core.pipeline.PipelineManager;
import io.aleph0.yap.core.pipeline.PipelineWrapper;
import io.aleph0.yap.core.task.TaskController;
import io.aleph0.yap.core.task.TaskManager;
import io.aleph0.yap.core.task.TaskManager.WorkerBody;
import io.aleph0.yap.core.transport.Channel;
import io.aleph0.yap.core.transport.Queue;
import io.aleph0.yap.core.transport.Topic;
import io.aleph0.yap.core.transport.channel.DefaultChannel;
import io.aleph0.yap.core.util.NoMetrics;
import io.aleph0.yap.core.worker.ConsumerWorkerFactory;
import io.aleph0.yap.core.worker.ProcessorWorkerFactory;
import io.aleph0.yap.core.worker.ProducerWorkerFactory;

public class PipelineBuilder {
  private static final AtomicInteger sequence = new AtomicInteger(1);

  private ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private PipelineControllerBuilder controller = DefaultPipelineController.builder();
  private final Map<String, TaskBuilder> tasks = new LinkedHashMap<>();
  private final List<PipelineWrapper> wrappers = new ArrayList<>();

  public PipelineBuilder setExecutor(ExecutorService executor) {
    if (executor == null)
      throw new IllegalArgumentException("executor must not be null");
    this.executor = executor;
    return this;
  }

  public PipelineBuilder setPipelineController(PipelineControllerBuilder controller) {
    if (controller == null)
      throw new IllegalArgumentException("controller must not be null");
    this.controller = controller;
    return this;
  }

  public <OutputT> ProducerTaskBuilder<OutputT, NoMetrics> addProducer(String id,
      ProducerWorker<OutputT> worker) {
    return addProducer(id, new ProducerWorkerFactory<OutputT, NoMetrics>() {
      @Override
      public ProducerWorker<OutputT> newProducerWorker() {
        return worker;
      }

      @Override
      public NoMetrics checkMetrics() {
        return NoMetrics.INSTANCE;
      }

      @Override
      public NoMetrics flushMetrics() {
        return NoMetrics.INSTANCE;
      }
    });
  }

  public <OutputT, MetricsT> ProducerTaskBuilder<OutputT, MetricsT> addProducer(String id,
      ProducerWorkerFactory<OutputT, MetricsT> workerFactory) {
    if (tasks.containsKey(id))
      throw new IllegalArgumentException("Task with id " + id + " already exists");
    final ProducerTaskBuilder<OutputT, MetricsT> result =
        new ProducerTaskBuilder<>(id, workerFactory);
    tasks.put(id, result);
    return result;
  }

  public <InputT, OutputT> ProcessorTaskBuilder<InputT, OutputT, NoMetrics> addProcessor(String id,
      ProcessorWorker<InputT, OutputT> body) {
    return addProcessor(id, new ProcessorWorkerFactory<InputT, OutputT, NoMetrics>() {
      @Override
      public ProcessorWorker<InputT, OutputT> newProcessorWorker() {
        return body;
      }

      @Override
      public NoMetrics checkMetrics() {
        return NoMetrics.INSTANCE;
      }

      @Override
      public NoMetrics flushMetrics() {
        return NoMetrics.INSTANCE;
      }
    });
  }

  public <InputT, OutputT, MetricsT> ProcessorTaskBuilder<InputT, OutputT, MetricsT> addProcessor(
      String id, ProcessorWorkerFactory<InputT, OutputT, MetricsT> workerFactory) {
    if (tasks.containsKey(id))
      throw new IllegalArgumentException("Task with id " + id + " already exists");
    final ProcessorTaskBuilder<InputT, OutputT, MetricsT> result =
        new ProcessorTaskBuilder<>(id, workerFactory);
    tasks.put(id, result);
    return result;
  }

  public <InputT> ConsumerTaskBuilder<InputT, NoMetrics> addConsumer(String id,
      ConsumerWorker<InputT> body) {
    return addConsumer(id, new ConsumerWorkerFactory<InputT, NoMetrics>() {
      @Override
      public ConsumerWorker<InputT> newConsumerWorker() {
        return body;
      }

      @Override
      public NoMetrics checkMetrics() {
        return NoMetrics.INSTANCE;
      }

      @Override
      public NoMetrics flushMetrics() {
        return NoMetrics.INSTANCE;
      }
    });
  }

  public <InputT, MetricsT> ConsumerTaskBuilder<InputT, MetricsT> addConsumer(String id,
      ConsumerWorkerFactory<InputT, MetricsT> bodyFactory) {
    if (tasks.containsKey(id))
      throw new IllegalArgumentException("Task with id " + id + " already exists");
    final ConsumerTaskBuilder<InputT, MetricsT> result = new ConsumerTaskBuilder<>(id, bodyFactory);
    tasks.put(id, result);
    return result;
  }

  public PipelineBuilder addWrapper(PipelineWrapper wrapper) {
    wrappers.add(wrapper);
    return this;
  }

  public Pipeline build() {
    // Create our channels
    final Map<String, List<Channel<?>>> subscriptions = new LinkedHashMap<>();
    final Map<String, List<Channel<?>>> subscribers = new LinkedHashMap<>();
    for (TaskBuilder task : tasks.values()) {
      switch (task) {
        case ProducerTaskBuilder<?, ?> producer:
          for (String subscriber : producer.subscribers) {
            final Channel<?> channel = new DefaultChannel<>();
            subscriptions.computeIfAbsent(producer.id, k -> new ArrayList<>()).add(channel);
            subscribers.computeIfAbsent(subscriber, k -> new ArrayList<>()).add(channel);
          }
          break;
        case ProcessorTaskBuilder<?, ?, ?> processor:
          for (String subscriber : processor.subscribers) {
            final Channel<?> channel = new DefaultChannel<>();
            subscriptions.computeIfAbsent(processor.id, k -> new ArrayList<>()).add(channel);
            subscribers.computeIfAbsent(subscriber, k -> new ArrayList<>()).add(channel);
          }
          break;
        case ConsumerTaskBuilder<?, ?> consumer:
          // Producers create channels, not consumers.
          break;
      }
    }

    // Create our topics
    final Map<String, Topic<?>> topics = new LinkedHashMap<>();
    for (TaskBuilder task : tasks.values()) {
      switch (task) {
        case ProducerTaskBuilder<?, ?> producer:
          topics.put(producer.id, producer.topic.build((List) subscriptions.get(producer.id)));
          break;
        case ProcessorTaskBuilder<?, ?, ?> processor:
          topics.put(processor.id, processor.topic.build((List) subscriptions.get(processor.id)));
          break;
        case ConsumerTaskBuilder<?, ?> consumer:
          // consumers have no topic
          break;
      }
    }

    // Create our queues
    final Map<String, Queue<?>> queues = new LinkedHashMap<>();
    for (TaskBuilder task : tasks.values()) {
      switch (task) {
        case ProducerTaskBuilder<?, ?> producer:
          // producers have no queue
          break;
        case ProcessorTaskBuilder<?, ?, ?> processor:
          queues.put(processor.id, processor.queue.build((List) subscribers.get(processor.id)));
          break;
        case ConsumerTaskBuilder<?, ?> consumer:
          queues.put(consumer.id, consumer.queue.build((List) subscribers.get(consumer.id)));
          break;
      }
    }

    // Subscribe our queues to our topics
    final List<TaskManager<?>> taskBodies = new ArrayList<>();
    for (TaskBuilder task : tasks.values()) {
      final String id = task.getId();

      final TaskManager<?> runner;
      switch (task) {
        case ProducerTaskBuilder<?, ?> producer: {
          final Topic topic = topics.get(id);
          final TaskController controller = producer.controller.build(topic);
          final TaskManager.WorkerBodyFactory<?> workerBodyFactory =
              new io.aleph0.yap.core.task.TaskManager.WorkerBodyFactory<>() {
                @Override
                public WorkerBody newWorkerBody() {
                  final ProducerWorker body = producer.workerFactory.newProducerWorker();
                  return () -> {
                    body.produce(topic::publish);
                  };
                }

                @Override
                public Object checkMetrics() {
                  return producer.workerFactory.checkMetrics();
                }

                @Override
                public Object flushMetrics() {
                  return producer.workerFactory.flushMetrics();
                }
              };
          runner = new TaskManager<>(task.getId(), producer.subscribers, executor, controller,
              workerBodyFactory, null, topic);
          break;
        }
        case ProcessorTaskBuilder<?, ?, ?> processor: {
          final Topic topic = topics.get(id);
          final Queue queue = queues.get(id);
          final TaskController controller = processor.controller.build(queue, topic);
          final TaskManager.WorkerBodyFactory<?> workerBodyFactory =
              new io.aleph0.yap.core.task.TaskManager.WorkerBodyFactory<>() {
                @Override
                public WorkerBody newWorkerBody() {
                  final ProcessorWorker body = processor.workerFactory.newProcessorWorker();
                  return () -> {
                    body.process(new Source() {
                      @Override
                      public Object tryTake() {
                        return queue.tryReceive();
                      }

                      @Override
                      public Object take(Duration timeout)
                          throws InterruptedException, TimeoutException {
                        return queue.receive(timeout);
                      }

                      @Override
                      public Object take() throws InterruptedException {
                        return queue.receive();
                      }
                    }, topic::publish);
                  };
                }

                @Override
                public Object checkMetrics() {
                  return processor.workerFactory.checkMetrics();
                }

                @Override
                public Object flushMetrics() {
                  return processor.workerFactory.flushMetrics();
                }
              };
          runner = new TaskManager<>(task.getId(), processor.subscribers, executor, controller,
              workerBodyFactory, queue, topic);
          break;
        }
        case ConsumerTaskBuilder<?, ?> consumer: {
          final Queue queue = queues.get(id);
          final TaskController controller = consumer.controller.build(queue);
          final TaskManager.WorkerBodyFactory<?> workerBodyFactory =
              new io.aleph0.yap.core.task.TaskManager.WorkerBodyFactory<>() {
                @Override
                public WorkerBody newWorkerBody() {
                  final ConsumerWorker body = consumer.workerFactory.newConsumerWorker();
                  return () -> {
                    body.consume(new Source() {
                      @Override
                      public Object tryTake() {
                        return queue.tryReceive();
                      }

                      @Override
                      public Object take(Duration timeout)
                          throws InterruptedException, TimeoutException {
                        return queue.receive(timeout);
                      }

                      @Override
                      public Object take() throws InterruptedException {
                        return queue.receive();
                      }
                    });
                  };
                }

                @Override
                public Object checkMetrics() {
                  return consumer.workerFactory.checkMetrics();
                }

                @Override
                public Object flushMetrics() {
                  return consumer.workerFactory.flushMetrics();
                }
              };
          runner = new TaskManager(task.getId(), Set.of(), executor, controller, workerBodyFactory,
              queue, null);
          break;
        }
      }

      taskBodies.add(runner);
    }

    final Map<String, Set<String>> graph =
        taskBodies.stream().map(t -> Map.entry(t.getId(), t.getSubscribers()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

    final PipelineController pipelineController = controller.build(graph);

    final int id = sequence.getAndIncrement();

    Pipeline result =
        new DefaultPipeline(new PipelineManager(id, executor, pipelineController, taskBodies));
    for (PipelineWrapper wrapper : wrappers)
      result = wrapper.wrapPipeline(result);

    return result;
  }

  public Pipeline buildAndStart() {
    final Pipeline result = build();
    result.start();
    return result;
  }

  public Future<?> buildAndStartAsFuture() {
    return buildAndStartAsCompletableFuture();
  }

  public CompletableFuture<Void> buildAndStartAsCompletableFuture() {
    final Pipeline pipeline = build();
    final CompletableFuture<Void> result = new CompletableFuture<>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        pipeline.cancel();
        return super.cancel(mayInterruptIfRunning);
      }
    };
    pipeline.addLifecycleListener(new Pipeline.LifecycleListener() {
      @Override
      public void onPipelineCompleted(int pipelineId) {
        result.complete(null);
      }

      @Override
      public void onPipelineFailed(int pipelineId, Throwable cause) {
        result.completeExceptionally(cause);
      }
    });
    pipeline.start();
    return result;
  }
}
