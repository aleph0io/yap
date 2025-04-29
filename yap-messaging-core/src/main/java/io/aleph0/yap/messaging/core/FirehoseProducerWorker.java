package io.aleph0.yap.messaging.core;

import io.aleph0.yap.core.worker.MeasuredProducerWorker;

public interface FirehoseProducerWorker<OutputT>
    extends MeasuredProducerWorker<OutputT, FirehoseMetrics> {
}
