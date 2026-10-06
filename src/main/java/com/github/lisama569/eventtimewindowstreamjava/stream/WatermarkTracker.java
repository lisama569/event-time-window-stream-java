package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按分区跟踪最大事件时间，全局水位线 = 各分区最大事件时间的最小值。
 * 水位线只增不减；分区之间互不影响推进，但全局水位线受最慢分区约束。
 */
public class WatermarkTracker {

    private final long allowedLatenessMillis;
    private final Map<Integer, Long> maxEventTimePerPartition = new ConcurrentHashMap<>();
    private volatile long watermark = Long.MIN_VALUE;

    public WatermarkTracker(long allowedLatenessMillis) {
        this.allowedLatenessMillis = allowedLatenessMillis;
    }

    /** 用一条已接受事件的事件时间推进对应分区，返回推进后的全局水位线。 */
    public synchronized long observe(int partition, long eventTimeMillis) {
        maxEventTimePerPartition.merge(partition, eventTimeMillis, Math::max);
        long min = maxEventTimePerPartition.values().stream().mapToLong(Long::longValue).min().orElse(Long.MIN_VALUE);
        if (min > watermark) {
            watermark = min;
        }
        return watermark;
    }

    /** 当前全局水位线。 */
    public long currentWatermark() {
        return watermark;
    }

    /** 假设用该事件时间推进对应分区后的全局水位线（不产生副作用）。 */
    public synchronized long prospectiveWatermark(int partition, long eventTimeMillis) {
        long min = Long.MAX_VALUE;
        for (var entry : maxEventTimePerPartition.entrySet()) {
            long v = entry.getKey() == partition
                    ? Math.max(entry.getValue(), eventTimeMillis)
                    : entry.getValue();
            min = Math.min(min, v);
        }
        if (!maxEventTimePerPartition.containsKey(partition)) {
            min = Math.min(min, eventTimeMillis);
        }
        long prospective = Math.max(watermark, min == Long.MAX_VALUE ? Long.MIN_VALUE : min);
        return prospective;
    }

    /**
     * 判断事件时间是否已超出允许迟到范围：
     * eventTime &lt; watermark - allowedLateness 视为不可容忍的迟到。
     */
    public boolean isBeyondAllowedLateness(long eventTimeMillis) {
        if (watermark == Long.MIN_VALUE) {
            return false;
        }
        return eventTimeMillis < watermark - allowedLatenessMillis;
    }

    public long allowedLatenessMillis() {
        return allowedLatenessMillis;
    }

    /** 各分区已见最大事件时间的副本，供进度快照使用。 */
    public synchronized Map<Integer, Long> partitionMaxEventTimes() {
        return Map.copyOf(maxEventTimePerPartition);
    }

    /** 是否已经见过该分区。 */
    public synchronized boolean knowsPartition(int partition) {
        return maxEventTimePerPartition.containsKey(partition);
    }

    /** 当前已知分区数量（有界）。 */
    public synchronized int partitionCount() {
        return maxEventTimePerPartition.size();
    }
}
