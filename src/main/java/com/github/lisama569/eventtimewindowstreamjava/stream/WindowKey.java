package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 窗口标识：按租户隔离的固定长度事件时间窗口 [start, start+size)。
 */
public record WindowKey(String tenant, long windowStart, long windowSize) {

    public long windowEnd() {
        return windowStart + windowSize;
    }
}
