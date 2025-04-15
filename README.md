# yap (Yet Another Pipeline)

**yap** is a lightweight Java 21+ library for building simple, local, imperative-style concurrent processing pipelines lifecycle observability.

## Quickstart

Here’s a quick example of how to use yap.

    public class Example {
        public static void main(String[] args) throws Exception {
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
        
            pb.buildAndStart().await();
        }
    }
    
## When to Use yap

- **Simple Pipelines**: When you need a lightweight framework for orchestrating concurrent tasks in a local environment.
- **Imperative Style**: When you prefer an imperative programming model over reactive or declarative approaches.
- **Observability**: When you need built-in observability to monitor task and pipeline states.
- **Custom Control**: When you want fine-grained control over task execution and transitions.

## When Not to Use yap

- **Distributed Systems**: If you need a distributed, fault-tolerant system, consider Apache Storm or Apache Flink.
- **Reactive Programming**: If you prefer a reactive programming model, Java 9+ Flows or Project Reactor may be a better fit.
- **Complex Data Processing**: For large-scale data processing with advanced features like windowing, state management, or event time processing, use Apache Flink or Apache Beam.
- **Stream Processing**: If you need a framework specifically designed for stream processing, consider Kafka Streams or Apache Flink.

## Comparison to Other Data Pipeline Alternatives

| Feature                     | yap                          | Apache Storm       | Apache Flink       | Java Streams       | Java Flows         |
|-----------------------------|------------------------------|--------------------|--------------------|--------------------|--------------------|
| **Programming Model**       | Imperative                  | Declarative        | Declarative        | Declarative        | Reactive           |
| **Concurrency**             | Local, thread-based         | Distributed        | Distributed        | Local              | Local              |
| **Observability**           | Built-in                    | Limited            | Advanced           | None               | None               |
| **Ease of Use**             | Simple                      | Moderate           | Complex            | Simple             | Moderate           |
| **Distributed Support**     | No                          | Yes                | Yes                | No                 | No                 |
| **Stream Processing**       | No                          | Yes                | Yes                | Limited            | Limited            |
| **Fault Tolerance**         | No                          | Yes                | Yes                | No                 | No                 |

yap is ideal for local, lightweight, and imperative pipelines, while other frameworks are better suited for distributed, reactive, or large-scale data processing needs.