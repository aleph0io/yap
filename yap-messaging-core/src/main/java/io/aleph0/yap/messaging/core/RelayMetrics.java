package io.aleph0.yap.messaging.core;

public record RelayMetrics(long submitted, long acknowledged, long awaiting) {

}
