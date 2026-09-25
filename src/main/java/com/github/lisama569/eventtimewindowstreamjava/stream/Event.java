package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 一条待处理事件。eventTime 为事件时间（epoch 毫秒）。
 */
public record Event(String eventId, String tenantId, int partition, long eventTime, long value) {
}
