package io.aleph0.yap.messaging.core;

import io.aleph0.yap.core.worker.MeasuredProcessorWorker;

public interface RelayProcessorWorker<InputT, OutputT>
    extends MeasuredProcessorWorker<InputT, OutputT, RelayMetrics> {

}
