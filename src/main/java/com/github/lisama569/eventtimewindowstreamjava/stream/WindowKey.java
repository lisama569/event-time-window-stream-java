package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 窗口标识：租户 + 窗口起止（事件时间，左闭右开）。
 */
public record WindowKey(String tenantId, long windowStart, long windowEnd) {
}
