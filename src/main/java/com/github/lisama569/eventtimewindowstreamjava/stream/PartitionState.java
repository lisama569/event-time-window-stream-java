package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 单个 (租户, 分区) 的隔离状态。
 *
 * <p>语义：
 * <ul>
 *   <li>水位线 = 已见最大事件时间 - 允许迟到毫秒数；水位线只增不减。</li>
 *   <li>事件时间 &lt;= 水位线的事件视为超出允许迟到范围，按 REJECTED_LATE 拒绝（不静默丢弃）。</li>
 *   <li>窗口在水位线 &gt;= 窗口右边界后确定（finalized）。</li>
 *   <li>去重索引为有界 LRU，重复 eventId 幂等忽略（DUPLICATE）。</li>
 *   <li>窗口数量超过 maxOpenWindows 时淘汰最旧的已确定窗口，保证状态有界。</li>
 * </ul>
 * 所有方法 synchronized，分区内部串行、分区之间并行且互不影响。
 */
class PartitionState {
    private final String tenantId;
    private final int partition;
    private final long windowSizeMs;
    private final long allowedLatenessMs;
    private final int maxOpenWindows;

    private long maxEventTimeSeen = Long.MIN_VALUE;
    private long watermark = Long.MIN_VALUE;

    /** 按窗口起点排序的窗口状态 */
    private final TreeMap<Long, WindowState> windows = new TreeMap<>();

    /** 有界 LRU 去重索引：eventId -> true */
    private final LinkedHashMap<String, Boolean> dedup;

    PartitionState(String tenantId, int partition, long windowSizeMs, long allowedLatenessMs,
                   int maxOpenWindows, int dedupCapacity) {
        this.tenantId = tenantId;
        this.partition = partition;
        this.windowSizeMs = windowSizeMs;
        this.allowedLatenessMs = allowedLatenessMs;
        this.maxOpenWindows = maxOpenWindows;
        this.dedup = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > dedupCapacity;
            }
        };
    }

    synchronized IngestResult ingest(Event event) {
        // 1. 去重：重复投递幂等忽略
        if (dedup.containsKey(event.eventId())) {
            return IngestResult.of(IngestStatus.DUPLICATE,
                    "eventId=%s 已处理过，幂等忽略".formatted(event.eventId()));
        }
        // 2. 迟到判定：事件时间 <= 水位线，超出允许迟到范围
        if (event.eventTime() <= watermark) {
            return IngestResult.of(IngestStatus.REJECTED_LATE,
                    "eventTime=%d 不晚于当前水位线=%d（允许迟到=%dms），窗口已确定，拒绝计入"
                            .formatted(event.eventTime(), watermark, allowedLatenessMs));
        }
        // 3. 推进水位线（只增不减）
        if (event.eventTime() > maxEventTimeSeen) {
            maxEventTimeSeen = event.eventTime();
            watermark = maxEventTimeSeen - allowedLatenessMs;
        }
        // 4. 计入窗口（左闭右开）
        long windowStart = event.eventTime() - Math.floorMod(event.eventTime(), windowSizeMs);
        WindowKey key = new WindowKey(tenantId, windowStart, windowStart + windowSizeMs);
        windows.computeIfAbsent(windowStart, ws -> new WindowState(key)).add(event.value());
        dedup.put(event.eventId(), Boolean.TRUE);
        // 5. 有界状态：淘汰最旧的已确定窗口
        evictFinalizedOverflow();
        return IngestResult.of(IngestStatus.ACCEPTED,
                "计入窗口 [%d, %d)，当前水位线=%d".formatted(windowStart, windowStart + windowSizeMs, watermark));
    }

    /** 淘汰超出 maxOpenWindows 的最旧已确定窗口，防止状态无界增长 */
    private void evictFinalizedOverflow() {
        while (windows.size() > maxOpenWindows) {
            Map.Entry<Long, WindowState> eldest = windows.firstEntry();
            if (eldest.getKey() + windowSizeMs <= watermark) {
                windows.pollFirstEntry();
            } else {
                break; // 最旧窗口尚未确定，不能淘汰（交由背压/拒绝语义在入口层处理）
            }
        }
    }

    synchronized long watermark() {
        return watermark;
    }

    synchronized List<WindowResult> windows() {
        List<WindowResult> out = new ArrayList<>();
        for (WindowState state : windows.values()) {
            boolean finalized = state.key().windowEnd() <= watermark;
            out.add(state.toResult(finalized));
        }
        out.sort(Comparator.comparing(r -> r.key().windowStart()));
        return out;
    }
}
