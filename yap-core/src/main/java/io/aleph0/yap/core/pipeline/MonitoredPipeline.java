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

import static java.util.Objects.requireNonNull;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.aleph0.yap.core.Pipeline;

public class MonitoredPipeline implements Pipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(MonitoredPipeline.class);

  public static PipelineWrapper newWrapper() {
    return pipeline -> new MonitoredPipeline(pipeline, MetricsReporter.stderr());
  }

  public static PipelineWrapper newWrapper(MetricsReporter reporter) {
    return pipeline -> new MonitoredPipeline(pipeline, reporter);
  }

  public static PipelineWrapper newWrapper(MetricsReporter reporter, Duration period) {
    return pipeline -> new MonitoredPipeline(pipeline, reporter, period);
  }

  private static final ScheduledExecutorService DEFAULT_SCHEDULER =
      Executors.newSingleThreadScheduledExecutor();

  public static final Duration DEFAULT_PERIOD = Duration.ofMinutes(1);


  @FunctionalInterface
  public static interface MetricsReporter {
    public static MetricsReporter stdout() {
      return metrics -> {
        System.out.println(metrics.toString());
      };
    }

    public static MetricsReporter stderr() {
      return metrics -> {
        System.err.println(metrics.toString());
      };
    }

    void reportMetrics(Pipeline.Metrics metrics);
  }

  private final ScheduledExecutorService scheduler;

  /**
   * The pipeline to be wrapped
   */
  private final Pipeline delegate;

  private final MetricsReporter reporter;

  /**
   * How often to collect and report metrics
   */
  private final Duration period;

  private volatile ScheduledFuture<?> reporting;

  public MonitoredPipeline(Pipeline delegate, MetricsReporter reporter) {
    this(delegate, reporter, DEFAULT_PERIOD);
  }

  public MonitoredPipeline(Pipeline delegate, MetricsReporter reporter, Duration period) {
    this(DEFAULT_SCHEDULER, delegate, reporter, period);
  }

  public MonitoredPipeline(ScheduledExecutorService scheduler, Pipeline delegate,
      MetricsReporter reporter, Duration period) {
    this.scheduler = requireNonNull(scheduler);
    this.delegate = requireNonNull(delegate);
    this.reporter = requireNonNull(reporter);
    this.period = requireNonNull(period);
  }

  @Override
  public int getId() {
    return delegate.getId();
  }

  @Override
  public void addLifecycleListener(LifecycleListener listener) {
    delegate.addLifecycleListener(listener);
  }

  @Override
  public void removeLifecycleListener(LifecycleListener listener) {
    delegate.removeLifecycleListener(listener);
  }

  @Override
  public void start() {
    // Add a lifecycle listener
    delegate.addLifecycleListener(new Pipeline.LifecycleListener() {
      @Override
      public void onPipelineStarted(int pipeline) {
        startReporting();
      }

      private void startReporting() {
        reporting = scheduler.scheduleAtFixedRate(MonitoredPipeline.this::reportMetrics, 0,
            period.toMillis(), TimeUnit.MILLISECONDS);
      }

      @Override
      public void onPipelineCompleted(int pipeline) {
        stopReporting();
      }

      @Override
      public void onPipelineCancelled(int pipeline) {
        stopReporting();
      }

      @Override
      public void onPipelineFailed(int pipeline, Throwable cause) {
        stopReporting();
      }

      private void stopReporting() {
        if (reporting != null) {
          reporting.cancel(false);
          reporting = null;
        }
      }
    });

    // Start the delegate pipeline
    delegate.start();
  }

  @Override
  public void cancel() {
    delegate.cancel();
  }

  @Override
  public void await() throws InterruptedException, ExecutionException, CancellationException {
    delegate.await();
  }

  @Override
  public Metrics checkMetrics() {
    return delegate.checkMetrics();
  }

  @Override
  public Metrics flushMetrics() {
    return delegate.flushMetrics();
  }

  private void reportMetrics() {
    try {
      LOGGER.atDebug().addKeyValue("pipeline", getId()).addKeyValue("reporter", reporter)
          .addKeyValue("period", period).log("Collecting and reporting metrics");

      // Collect metrics from the delegate pipeline
      Pipeline.Metrics metrics = delegate.flushMetrics();

      // Report the collected metrics
      reporter.reportMetrics(metrics);
    } catch (Exception e) {
      // Handle any exceptions that occur during metric collection or reporting
      LOGGER.atError().addKeyValue("pipeline", getId()).addKeyValue("reporter", reporter)
          .addKeyValue("period", period).setCause(e).log("Error collecting or reporting metrics");
    }
  }
}
