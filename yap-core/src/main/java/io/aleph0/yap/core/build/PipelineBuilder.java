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
import io.aleph0.yap.core.Sink;
import io.aleph0.yap.core.Source;
import io.aleph0.yap.core.pipeline.DefaultPipeline;
import io.aleph0.yap.core.pipeline.DefaultPipelineController;
import io.aleph0.yap.core.pipeline.PipelineController;
import io.aleph0.yap.core.pipeline.PipelineManager;
import io.aleph0.yap.core.pipeline.PipelineWrapper;
import io.aleph0.yap.core.task.DefaultConsumerTaskController;
import io.aleph0.yap.core.task.DefaultProcessorTaskController;
import io.aleph0.yap.core.task.DefaultProducerTaskController;
import io.aleph0.yap.core.task.TaskController;
import io.aleph0.yap.core.task.TaskController.ProcessorTaskControllerBuilder;
import io.aleph0.yap.core.task.TaskController.ProducerTaskControllerBuilder;
import io.aleph0.yap.core.task.TaskManager;
import io.aleph0.yap.core.task.TaskManager.WorkerBody;
import io.aleph0.yap.core.transport.Channel;
import io.aleph0.yap.core.transport.Queue;
import io.aleph0.yap.core.transport.Topic;
import io.aleph0.yap.core.transport.channel.DefaultChannel;
import io.aleph0.yap.core.util.DirectedGraphs;
import io.aleph0.yap.core.util.NoMetrics;
import io.aleph0.yap.core.worker.ConsumerWorkerFactory;
import io.aleph0.yap.core.worker.MeasuredConsumerWorker;
import io.aleph0.yap.core.worker.MeasuredProcessorWorker;
import io.aleph0.yap.core.worker.MeasuredProducerWorker;
import io.aleph0.yap.core.worker.ProcessorWorkerFactory;
import io.aleph0.yap.core.worker.ProducerWorkerFactory;
import io.aleph0.yap.core.worker.SingletonConsumerWorkerFactory;
import io.aleph0.yap.core.worker.SingletonProcessorWorkerFactory;
import io.aleph0.yap.core.worker.SingletonProducerWorkerFactory;

public class PipelineBuilder {
  private static final AtomicInteger sequence = new AtomicInteger(1);

  private ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private PipelineControllerBuilder controller = DefaultPipelineController.builder();
  private final Map<String, TaskBuilder> tasks = new LinkedHashMap<>();
  private final List<PipelineWrapper> wrappers = new ArrayList<>();
  private final List<Pipeline.LifecycleListener> lifecycleListeners = new ArrayList<>();

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

  /**
   * Add a producer task to the pipeline. The producer task will only support a single worker, which
   * is the given worker. If the {@link TaskController} attempts to scale the task beyond one
   * worker, then the task will fail with an {@link IllegalStateException}.
   * 
   * @param <OutputT> the type of the producer's output
   * @param id the task ID
   * @param worker the producer worker
   * @return a {@link ProducerTaskBuilder} to configure the producer task
   * 
   * @see #addProducer(String, MeasuredProducerWorker)
   */
  public <OutputT> ProducerTaskBuilder<OutputT, NoMetrics> addProducer(String id,
      ProducerWorker<OutputT> worker) {
    return addProducer(id, MeasuredProducerWorker.withNoMetrics(worker));
  }

  /**
   * Add a producer task to the pipeline. The producer task will only support a single worker, which
   * is the given worker. If the {@link TaskController} attempts to scale the task beyond one
   * worker, then the task will fail with an {@link IllegalStateException}.
   * 
   * @param <OutputT> the type of the producer's output
   * @param <MetricsT> the type of the producer's metrics
   * @param id the task ID
   * @param worker the producer worker
   * @return a {@link ProducerTaskBuilder} to configure the producer task
   * 
   * @see SingletonProducerWorkerFactory
   */
  public <OutputT, MetricsT> ProducerTaskBuilder<OutputT, MetricsT> addProducer(String id,
      MeasuredProducerWorker<OutputT, MetricsT> worker) {
    if (worker == null)
      throw new NullPointerException("worker");
    return addProducer(id, new SingletonProducerWorkerFactory<OutputT, MetricsT>(worker));
  }

  /**
   * Add a producer task to the pipeline. The given {@link ProducerWorkerFactory workerFactory} will
   * be used to create new workers for the task as requested by the {@link TaskController}.
   * 
   * @param <OutputT> the type of the producer's output
   * @param <MetricsT> the type of the producer's metrics
   * @param id the task ID
   * @param workerFactory the producer worker factory
   * @return a {@link ProducerTaskBuilder} to configure the producer task
   * 
   * @see ProducerTaskBuilder#setController(ProducerTaskControllerBuilder)
   * @see DefaultProducerTaskController#builder()
   */
  public <OutputT, MetricsT> ProducerTaskBuilder<OutputT, MetricsT> addProducer(String id,
      ProducerWorkerFactory<OutputT, MetricsT> workerFactory) {
    if (id == null)
      throw new NullPointerException("id");
    if (workerFactory == null)
      throw new NullPointerException("workerFactory");
    if (tasks.containsKey(id))
      throw new IllegalArgumentException("Task with id " + id + " already exists");
    final ProducerTaskBuilder<OutputT, MetricsT> result =
        new ProducerTaskBuilder<>(id, workerFactory);
    tasks.put(id, result);
    return result;
  }

  /**
   * Add a processor task to the pipeline. The processor task will only support a single worker,
   * which is the given worker. If the {@link TaskController} attempts to scale the task beyond one
   * worker, then the task will fail with an {@link IllegalStateException}.
   * 
   * @param <InputT> the type of the processor's input
   * @param <OutputT> the type of the processor's output
   * @param id the task ID
   * @param worker the processor worker
   * @return a {@link ProcessorTaskBuilder} to configure the processor task
   * 
   * @see #addProcessor(String, MeasuredProcessorWorker)
   */
  public <InputT, OutputT> ProcessorTaskBuilder<InputT, OutputT, NoMetrics> addProcessor(String id,
      ProcessorWorker<InputT, OutputT> worker) {
    return addProcessor(id, MeasuredProcessorWorker.withNoMetrics(worker));
  }

  /**
   * Add a processor task to the pipeline. The processor task will only support a single worker,
   * which is the given worker. If the {@link TaskController} attempts to scale the task beyond one
   * worker, then the task will fail with an {@link IllegalStateException}.
   * 
   * @param <InputT> the type of the processor's input
   * @param <OutputT> the type of the processor's output
   * @param <MetricsT> the type of the processor's metrics
   * @param id the task ID
   * @param worker the processor worker
   * @return a {@link ProcessorTaskBuilder} to configure the processor task
   * 
   * @see SingletonProcessorWorkerFactory
   */
  public <InputT, OutputT, MetricsT> ProcessorTaskBuilder<InputT, OutputT, MetricsT> addProcessor(
      String id, MeasuredProcessorWorker<InputT, OutputT, MetricsT> worker) {
    if (worker == null)
      throw new NullPointerException("worker");
    return addProcessor(id, new SingletonProcessorWorkerFactory<InputT, OutputT, MetricsT>(worker));
  }

  /**
   * Add a processor task to the pipeline. The given {@link ProcessorWorkerFactory workerFactory}
   * will be used to create new workers for the task as requested by the {@link TaskController}.
   * 
   * @param <InputT> the type of the processor's input
   * @param <OutputT> the type of the processor's output
   * @param <MetricsT> the type of the processor's metrics
   * @param id the task ID
   * @param workerFactory the processor worker factory
   * @return a {@link ProcessorTaskBuilder} to configure the processor task
   * 
   * @see ProcessorTaskBuilder#setController(ProcessorTaskControllerBuilder)
   * @see DefaultProcessorTaskController#builder()
   */
  public <InputT, OutputT, MetricsT> ProcessorTaskBuilder<InputT, OutputT, MetricsT> addProcessor(
      String id, ProcessorWorkerFactory<InputT, OutputT, MetricsT> workerFactory) {
    if (id == null)
      throw new NullPointerException("id");
    if (workerFactory == null)
      throw new NullPointerException("workerFactory");
    if (tasks.containsKey(id))
      throw new IllegalArgumentException("Task with id " + id + " already exists");
    final ProcessorTaskBuilder<InputT, OutputT, MetricsT> result =
        new ProcessorTaskBuilder<>(id, workerFactory);
    tasks.put(id, result);
    return result;
  }

  /**
   * Add a consumer task to the pipeline. The consumer task will only support a single worker, which
   * is the given worker. If the {@link TaskController} attempts to scale the task beyond one
   * worker, then the task will fail with an {@link IllegalStateException}.
   * 
   * @param <InputT> the type of the consumer's input
   * @param id the task ID
   * @param worker the consumer worker
   * @return a {@link ConsumerTaskBuilder} to configure the consumer task
   * 
   * @see #addConsumer(String, MeasuredConsumerWorker)
   */
  public <InputT> ConsumerTaskBuilder<InputT, NoMetrics> addConsumer(String id,
      ConsumerWorker<InputT> worker) {
    return addConsumer(id, MeasuredConsumerWorker.withNoMetrics(worker));
  }

  /**
   * Add a consumer task to the pipeline. The consumer task will only support a single worker, which
   * is the given worker. If the {@link TaskController} attempts to scale the task beyond one
   * worker, then the task will fail with an {@link IllegalStateException}.
   * 
   * @param <InputT> the type of the consumer's input
   * @param <MetricsT> the type of the consumer's metrics
   * @param id the task ID
   * @param worker the consumer worker
   * @return a {@link ConsumerTaskBuilder} to configure the consumer task
   * 
   * @see SingletonConsumerWorkerFactory
   */
  public <InputT, MetricsT> ConsumerTaskBuilder<InputT, MetricsT> addConsumer(String id,
      MeasuredConsumerWorker<InputT, MetricsT> worker) {
    if (worker == null)
      throw new NullPointerException("worker");
    return addConsumer(id, new SingletonConsumerWorkerFactory<InputT, MetricsT>(worker));
  }

  /**
   * Add a consumer task to the pipeline. The given {@link ConsumerWorkerFactory workerFactory} will
   * be used to create new workers for the task as requested by the {@link TaskController}.
   * 
   * @param <InputT> the type of the consumer's input
   * @param <MetricsT> the type of the consumer's metrics
   * @param id the task ID
   * @param workerFactory the consumer worker factory
   * @return a {@link ConsumerTaskBuilder} to configure the consumer task
   * 
   * @see ConsumerTaskBuilder#setController(TaskControllerBuilder)
   * @see DefaultConsumerTaskController#builder()
   */
  public <InputT, MetricsT> ConsumerTaskBuilder<InputT, MetricsT> addConsumer(String id,
      ConsumerWorkerFactory<InputT, MetricsT> workerFactory) {
    if (id == null)
      throw new NullPointerException("id");
    if (workerFactory == null)
      throw new NullPointerException("workerFactory");
    if (tasks.containsKey(id))
      throw new IllegalArgumentException("Task with id " + id + " already exists");
    final ConsumerTaskBuilder<InputT, MetricsT> result =
        new ConsumerTaskBuilder<>(id, workerFactory);
    tasks.put(id, result);
    return result;
  }

  public PipelineBuilder addWrapper(PipelineWrapper wrapper) {
    wrappers.add(wrapper);
    return this;
  }

  public PipelineBuilder addLifecycleListener(Pipeline.LifecycleListener listener) {
    lifecycleListeners.add(listener);
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
          if (!subscriptions.containsKey(producer.id))
            throw new IllegalArgumentException("Producer " + producer.id + " has no subscribers");
          topics.put(producer.id, producer.topic.build((List) subscriptions.get(producer.id)));
          break;
        case ProcessorTaskBuilder<?, ?, ?> processor:
          if (!subscriptions.containsKey(processor.id))
            throw new IllegalArgumentException("Processor " + processor.id + " has no subscribers");
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
          if (!subscribers.containsKey(processor.id))
            throw new IllegalArgumentException("Processor " + processor.id + " has no producers");
          queues.put(processor.id, processor.queue.build((List) subscribers.get(processor.id)));
          break;
        case ConsumerTaskBuilder<?, ?> consumer:
          if (!subscribers.containsKey(consumer.id))
            throw new IllegalArgumentException("Consumer " + consumer.id + " has no producers");
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
                    body.produce(new Sink() {
                      @Override
                      public void put(Object value) throws InterruptedException {
                        topic.publish(value);
                      }
                    });
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
                    }, new Sink() {
                      @Override
                      public void put(Object value) throws InterruptedException {
                        topic.publish(value);
                      }

                    });
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
        case

            ConsumerTaskBuilder<?, ?> consumer: {
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

    if (!DirectedGraphs.isWeaklyConnected(graph))
      throw new IllegalArgumentException("Pipeline contains disconnected node(s)");

    DirectedGraphs.findCycle(graph).ifPresent(cycle -> {
      throw new IllegalArgumentException(
          "Pipeline contains cycle(s): " + String.join(" -> ", cycle));
    });

    final PipelineController pipelineController = controller.build(graph);

    final int id = sequence.getAndIncrement();

    Pipeline result =
        new DefaultPipeline(new PipelineManager(id, executor, pipelineController, taskBodies));
    for (PipelineWrapper wrapper : wrappers)
      result = wrapper.wrapPipeline(result);

    for (Pipeline.LifecycleListener listener : lifecycleListeners)
      result.addLifecycleListener(listener);

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
