package com.github.lisama569.eventtimewindowstreamjava.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 进程内实时事件流处理器。
 *
 * <p>语义：
 * <ul>
 *   <li>事件按事件时间归入租户的固定窗口 [start, start+size)。</li>
 *   <li>水位线 = 各分区最大事件时间的最小值，只增不减；
 *       窗口在其 windowEnd + allowedLateness &lt;= 水位线时关闭并输出确定结果，
 *       此后任何属于该窗口的事件必然被判定为超出允许迟到范围，结果不可再变。</li>
 *   <li>事件时间 &lt; 水位线 - allowedLateness 的事件以 LATE_BEYOND_ALLOWED 拒绝，不静默丢弃。</li>
 *   <li>相同 eventId 重复投递以 DUPLICATE 拒绝，幂等不重复计数。</li>
 *   <li>在途（已接受但所属窗口尚未关闭）事件数达到 maxInFlight 时以
 *       BACKPRESSURE_CAPACITY 拒绝，生产端需降速；窗口关闭后在途数释放。</li>
 *   <li>去重记录与结果缓冲均有界，长期运行无无界状态增长。</li>
 * </ul>
 * 线程安全：所有状态迁移在同一把锁内完成，支持多分区并发写入。
 */
public class EventStreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(EventStreamProcessor.class);
    private static final int MAX_RETAINED_RESULTS = 1024;

    private final long windowSizeMillis;
    private final int maxInFlight;
    private final WatermarkTracker watermarkTracker;
    private final DedupStore dedupStore;

    private final Object lock = new Object();
    /** 租户窗口的进行中聚合：[count, sum]。 */
    private final Map<WindowKey, long[]> openWindows = new HashMap<>();
    private final Deque<WindowResult> finalized = new ArrayDeque<>();
    private int inFlight;

    public EventStreamProcessor(long windowSizeMillis, long allowedLatenessMillis,
                                int maxInFlight, int maxDedupEntries) {
        this.windowSizeMillis = windowSizeMillis;
        this.maxInFlight = maxInFlight;
        this.watermarkTracker = new WatermarkTracker(allowedLatenessMillis);
        this.dedupStore = new DedupStore(maxDedupEntries);
    }

    /** 接入一条事件，返回可区分原因的判定结果。 */
    public IngestOutcome ingest(Event event) {
        if (event == null || event.eventId() == null || event.eventId().isBlank()
                || event.tenant() == null || event.eventTimeMillis() < 0) {
            log.info("ingest REJECTED reason=INVALID event={}", event);
            return IngestOutcome.rejected(RejectionReason.INVALID);
        }
        synchronized (lock) {
            long wm = watermarkTracker.currentWatermark();
            if (watermarkTracker.isBeyondAllowedLateness(event.eventTimeMillis())) {
                log.info("ingest REJECTED reason=LATE_BEYOND_ALLOWED id={} eventTime={} watermark={} allowedLateness={}",
                        event.eventId(), event.eventTimeMillis(), wm, watermarkTracker.allowedLatenessMillis());
                return IngestOutcome.rejected(RejectionReason.LATE_BEYOND_ALLOWED);
            }
            if (dedupStore.contains(event.eventId())) {
                log.info("ingest REJECTED reason=DUPLICATE id={} eventTime={} watermark={}",
                        event.eventId(), event.eventTimeMillis(), wm);
                return IngestOutcome.rejected(RejectionReason.DUPLICATE);
            }
            if (inFlight >= maxInFlight && !wouldReleaseCapacity(event)) {
                log.info("ingest REJECTED reason=BACKPRESSURE_CAPACITY id={} inFlight={} maxInFlight={}",
                        event.eventId(), inFlight, maxInFlight);
                return IngestOutcome.rejected(RejectionReason.BACKPRESSURE_CAPACITY);
            }
            dedupStore.markIfAbsent(event.eventId());
            long newWatermark = watermarkTracker.observe(event.partition(), event.eventTimeMillis());
            WindowKey key = windowKeyOf(event);
            openWindows.computeIfAbsent(key, k -> new long[2]);
            long[] agg = openWindows.get(key);
            agg[0] += 1;
            agg[1] += event.amount();
            inFlight++;
            log.info("ingest ACCEPTED id={} tenant={} partition={} eventTime={} window=[{},{}) watermark={}->{} inFlight={}",
                    event.eventId(), event.tenant(), event.partition(), event.eventTimeMillis(),
                    key.windowStart(), key.windowEnd(), wm, newWatermark, inFlight);

            finalizeWindows(newWatermark);
            return IngestOutcome.accepted();
        }
    }

    private WindowKey windowKeyOf(Event event) {
        long start = Math.floorDiv(event.eventTimeMillis(), windowSizeMillis) * windowSizeMillis;
        return new WindowKey(event.tenant(), start, windowSizeMillis);
    }

    /**
     * 背压豁免判定：若接受该事件所推进的水位线能关闭至少一个窗口（释放容量），
     * 则即使在途已满也应放行，避免“容量已满 -> 无法推进水位线 -> 容量永不释放”的死锁。
     * 调用方须持有锁。
     */
    private boolean wouldReleaseCapacity(Event event) {
        long prospective = watermarkTracker.prospectiveWatermark(event.partition(), event.eventTimeMillis());
        long horizon = prospective - watermarkTracker.allowedLatenessMillis();
        return openWindows.keySet().stream().anyMatch(k -> k.windowEnd() <= horizon);
    }

    /** 关闭所有 windowEnd + allowedLateness &lt;= watermark 的窗口并输出结果。调用方须持有锁。 */
    private void finalizeWindows(long watermark) {
        long horizon = watermark - watermarkTracker.allowedLatenessMillis();
        var it = openWindows.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            WindowKey key = entry.getKey();
            if (key.windowEnd() <= horizon) {
                long[] agg = entry.getValue();
                WindowResult result = new WindowResult(key, agg[0], agg[1], watermark);
                finalized.addLast(result);
                while (finalized.size() > MAX_RETAINED_RESULTS) {
                    finalized.removeFirst();
                }
                inFlight -= agg[0];
                it.remove();
                log.info("window FINALIZED tenant={} window=[{},{}) count={} sum={} watermark={} inFlight={}",
                        key.tenant(), key.windowStart(), key.windowEnd(), agg[0], agg[1], watermark, inFlight);
            }
        }
    }

    /** 已确定（水位线越过窗口右边界 + 允许迟到）的窗口结果快照，按输出顺序返回。 */
    public List<WindowResult> finalizedResults() {
        synchronized (lock) {
            return new ArrayList<>(finalized);
        }
    }

    public long currentWatermark() {
        return watermarkTracker.currentWatermark();
    }

    public int inFlightCount() {
        synchronized (lock) {
            return inFlight;
        }
    }

    public int openWindowCount() {
        synchronized (lock) {
            return openWindows.size();
        }
    }
}
