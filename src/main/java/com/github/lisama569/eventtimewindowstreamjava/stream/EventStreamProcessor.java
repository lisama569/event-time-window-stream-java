package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事件流处理引擎。
 *
 * <p>按 (租户, 分区) 隔离状态：每个分区独立的 {@link PartitionState}，
 * 不同分区/租户可并发写入且互不串扰。跨分区查询时按窗口键合并结果，
 * 窗口只有在该租户所有分区的水位线都越过窗口右边界后才判定为确定。
 *
 * <p>{@link #reset()} 清空全部状态，从历史重放相同事件序列后结论与首次一致。
 */
public class EventStreamProcessor {
    private final long windowSizeMs;
    private final long allowedLatenessMs;
    private final int maxOpenWindows;
    private final int dedupCapacity;

    /** (tenantId, partition) -> 隔离的分区状态 */
    private final ConcurrentHashMap<String, PartitionState> partitions = new ConcurrentHashMap<>();

    public EventStreamProcessor(long windowSizeMs, long allowedLatenessMs,
                                int maxOpenWindows, int dedupCapacity) {
        this.windowSizeMs = windowSizeMs;
        this.allowedLatenessMs = allowedLatenessMs;
        this.maxOpenWindows = maxOpenWindows;
        this.dedupCapacity = dedupCapacity;
    }

    private static String stateKey(String tenantId, int partition) {
        return tenantId + "#" + partition;
    }

    private PartitionState stateOf(String tenantId, int partition) {
        return partitions.computeIfAbsent(stateKey(tenantId, partition),
                k -> new PartitionState(tenantId, partition, windowSizeMs, allowedLatenessMs,
                        maxOpenWindows, dedupCapacity));
    }

    /** 摄入单条事件，返回可区分原因的结果 */
    public IngestResult ingest(Event event) {
        return stateOf(event.tenantId(), event.partition()).ingest(event);
    }

    /** 查询租户窗口：跨分区按窗口键合并，finalized 需所有分区水位线越过窗口右边界 */
    public List<WindowResult> windows(String tenantId) {
        long minWatermark = minWatermark(tenantId);
        Map<WindowKey, long[]> merged = new HashMap<>(); // key -> [count, sum]
        for (PartitionState state : partitionsOf(tenantId)) {
            for (WindowResult r : state.windows()) {
                long[] acc = merged.computeIfAbsent(r.key(), k -> new long[2]);
                acc[0] += r.count();
                acc[1] += r.sum();
            }
        }
        List<WindowResult> out = new ArrayList<>();
        merged.forEach((key, acc) -> out.add(
                new WindowResult(key, acc[0], acc[1], key.windowEnd() <= minWatermark)));
        out.sort(Comparator.comparing(r -> r.key().windowStart()));
        return out;
    }

    /** 租户各分区当前水位线（key 为 "tenantId#partition"） */
    public Map<String, Long> watermarks(String tenantId) {
        Map<String, Long> out = new HashMap<>();
        String prefix = tenantId + "#";
        partitions.forEach((k, v) -> {
            if (k.startsWith(prefix)) {
                out.put(k, v.watermark());
            }
        });
        return out;
    }

    private List<PartitionState> partitionsOf(String tenantId) {
        List<PartitionState> out = new ArrayList<>();
        String prefix = tenantId + "#";
        partitions.forEach((k, v) -> {
            if (k.startsWith(prefix)) {
                out.add(v);
            }
        });
        return out;
    }

    private long minWatermark(String tenantId) {
        long min = Long.MAX_VALUE;
        for (PartitionState state : partitionsOf(tenantId)) {
            min = Math.min(min, state.watermark());
        }
        return min == Long.MAX_VALUE ? Long.MIN_VALUE : min;
    }

    /** 清空全部状态，用于重启/从历史重放 */
    public void reset() {
        partitions.clear();
    }
}
