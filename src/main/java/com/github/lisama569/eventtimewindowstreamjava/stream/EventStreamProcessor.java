package com.github.lisama569.eventtimewindowstreamjava.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 进程内实时事件流处理器（多租户版）。
 *
 * <p>语义：
 * <ul>
 *   <li>事件按事件时间归入租户的固定窗口 [start, start+size)。</li>
 *   <li><b>水位线按租户独立</b>：租户水位线 = 该租户各分区已见最大事件时间的最小值。
 *       窗口只在其 windowEnd + allowedLateness &lt;= 所属租户自己的水位线时关闭，
 *       因此安静租户不影响其他租户出结果，推进快的租户也不会把其他租户数据判晚。</li>
 *   <li>事件时间 &lt; 其租户水位线 - allowedLateness 的事件以 LATE_BEYOND_ALLOWED 拒绝。</li>
 *   <li>相同 eventId 重复投递以 DUPLICATE 拒绝，幂等不重复计数。</li>
 *   <li><b>租户级积压额度</b>：某租户未确定在途事件达到自身额度时仅以
 *       TENANT_BACKLOG_LIMIT 拒绝该租户，其他租户照常受理；被拒事件不记录去重，
 *       降速重投不会误判为重复。全局在途上限（BACKPRESSURE_CAPACITY）仍然保留。</li>
 *   <li>豁免规则：若接受某事件所推进的<b>该租户</b>水位线能关闭其至少一个窗口，
 *       即使额度/容量已满也放行，避免“满 → 无法推进 → 永不释放”的死锁。</li>
 *   <li>所有计数（去重、租户数、每租户分区数、结果缓冲）均有界。</li>
 * </ul>
 * 线程安全：所有状态迁移在同一把锁内完成，支持多租户多分区并发写入。
 */
public class EventStreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(EventStreamProcessor.class);
    private static final int MAX_RETAINED_RESULTS = 1024;
    private static final int DEFAULT_MAX_TENANTS = 1024;
    private static final int DEFAULT_MAX_PARTITIONS_PER_TENANT = 1024;

    private final long windowSizeMillis;
    private final int maxInFlight;
    private final int defaultTenantBacklog;
    private final Map<String, Integer> tenantBacklogOverrides;
    private final TenantWatermarkTracker watermarkTracker;
    private final DedupStore dedupStore;

    private final Object lock = new Object();
    /** 租户窗口的进行中聚合：[count, sum]。 */
    private final Map<WindowKey, long[]> openWindows = new HashMap<>();
    private final Deque<WindowResult> finalized = new ArrayDeque<>();
    private final int[] rejectedTotals = new int[RejectionReason.values().length];
    private int inFlight;

    public EventStreamProcessor(long windowSizeMillis, long allowedLatenessMillis,
                                int maxInFlight, int maxDedupEntries) {
        this(windowSizeMillis, allowedLatenessMillis, maxInFlight, maxInFlight,
                maxDedupEntries, Map.of(),
                DEFAULT_MAX_TENANTS, DEFAULT_MAX_PARTITIONS_PER_TENANT);
    }

    public EventStreamProcessor(long windowSizeMillis, long allowedLatenessMillis,
                                int maxInFlight, int defaultTenantBacklog, int maxDedupEntries) {
        this(windowSizeMillis, allowedLatenessMillis, maxInFlight, defaultTenantBacklog,
                maxDedupEntries, Map.of(),
                DEFAULT_MAX_TENANTS, DEFAULT_MAX_PARTITIONS_PER_TENANT);
    }

    public EventStreamProcessor(long windowSizeMillis, long allowedLatenessMillis,
                                int maxInFlight, int defaultTenantBacklog, int maxDedupEntries,
                                Map<String, Integer> tenantBacklogOverrides,
                                int maxTenants, int maxPartitionsPerTenant) {
        if (defaultTenantBacklog <= 0 || maxInFlight <= 0) {
            throw new IllegalArgumentException("capacity bounds must be positive");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.maxInFlight = maxInFlight;
        this.defaultTenantBacklog = defaultTenantBacklog;
        this.tenantBacklogOverrides = Map.copyOf(tenantBacklogOverrides);
        this.watermarkTracker = new TenantWatermarkTracker(
                allowedLatenessMillis, maxTenants, maxPartitionsPerTenant);
        this.dedupStore = new DedupStore(maxDedupEntries);
    }

    /** 接入一条事件，返回可区分原因的判定结果。 */
    public IngestOutcome ingest(Event event) {
        boolean structurallyInvalid = event == null || event.eventId() == null || event.eventId().isBlank()
                || event.tenant() == null || event.eventTimeMillis() < 0;
        if (structurallyInvalid) {
            synchronized (lock) {
                recordRejection(null, RejectionReason.INVALID);
            }
            log.info("ingest REJECTED reason=INVALID event={}", event);
            return IngestOutcome.rejected(RejectionReason.INVALID);
        }
        synchronized (lock) {
            if (watermarkTracker.tenantStateIfPresent(event.tenant()) == null && watermarkTracker.atCapacity()) {
                evictEldestTenant();
            }
            TenantWatermarkTracker.TenantState ts = watermarkTracker.tenantState(event.tenant());
            long wm = ts.watermark();
            if (ts.isBeyondAllowedLateness(event.eventTimeMillis(), watermarkTracker.allowedLatenessMillis())) {
                recordRejection(ts, RejectionReason.LATE_BEYOND_ALLOWED);
                log.info("ingest REJECTED reason=LATE_BEYOND_ALLOWED id={} tenant={} eventTime={} tenantWatermark={} allowedLateness={}",
                        event.eventId(), event.tenant(), event.eventTimeMillis(), wm,
                        watermarkTracker.allowedLatenessMillis());
                return IngestOutcome.rejected(RejectionReason.LATE_BEYOND_ALLOWED);
            }
            if (dedupStore.contains(event.eventId())) {
                recordRejection(ts, RejectionReason.DUPLICATE);
                log.info("ingest REJECTED reason=DUPLICATE id={} tenant={} eventTime={} tenantWatermark={}",
                        event.eventId(), event.tenant(), event.eventTimeMillis(), wm);
                return IngestOutcome.rejected(RejectionReason.DUPLICATE);
            }
            int tenantCap = tenantBacklog(event.tenant());
            if (ts.inFlight() >= tenantCap && !wouldReleaseCapacity(ts, event)) {
                recordRejection(ts, RejectionReason.TENANT_BACKLOG_LIMIT);
                log.info("ingest REJECTED reason=TENANT_BACKLOG_LIMIT id={} tenant={} tenantInFlight={} tenantCap={} (globalInFlight={}/{})",
                        event.eventId(), event.tenant(), ts.inFlight(), tenantCap, inFlight, maxInFlight);
                return IngestOutcome.rejected(RejectionReason.TENANT_BACKLOG_LIMIT);
            }
            if (inFlight >= maxInFlight && !wouldReleaseCapacity(ts, event)) {
                recordRejection(ts, RejectionReason.BACKPRESSURE_CAPACITY);
                log.info("ingest REJECTED reason=BACKPRESSURE_CAPACITY id={} tenant={} globalInFlight={} maxInFlight={}",
                        event.eventId(), event.tenant(), inFlight, maxInFlight);
                return IngestOutcome.rejected(RejectionReason.BACKPRESSURE_CAPACITY);
            }
            dedupStore.markIfAbsent(event.eventId());
            long newWatermark = ts.observe(event.partition(), event.eventTimeMillis());
            WindowKey key = windowKeyOf(event);
            openWindows.computeIfAbsent(key, k -> new long[2]);
            long[] agg = openWindows.get(key);
            agg[0] += 1;
            agg[1] += event.amount();
            inFlight++;
            ts.incrementInFlight();
            log.info("ingest ACCEPTED id={} tenant={} partition={} eventTime={} window=[{},{}) tenantWatermark={}->{} tenantInFlight={} globalInFlight={}",
                    event.eventId(), event.tenant(), event.partition(), event.eventTimeMillis(),
                    key.windowStart(), key.windowEnd(), wm, newWatermark, ts.inFlight(), inFlight);

            finalizeTenantWindows(event.tenant(), ts, newWatermark);
            return IngestOutcome.accepted();
        }
    }

    private void recordRejection(TenantWatermarkTracker.TenantState ts, RejectionReason reason) {
        rejectedTotals[reason.ordinal()]++;
        if (ts != null) {
            ts.recordRejection(reason);
        }
    }

    /**
     * 租户跟踪数有界淘汰：移除最久未写入的租户，同时丢弃其残留的未定稿窗口，
     * 并把对应在途计数从全局计数中扣除，保证全局计数与窗口表始终自洽。
     * 调用方须持有锁。
     */
    private void evictEldestTenant() {
        var victim = watermarkTracker.evictEldestTenant();
        if (victim == null) {
            return;
        }
        String tenant = victim.getKey();
        TenantWatermarkTracker.TenantState state = victim.getValue();
        int removedEvents = 0;
        int removedWindows = 0;
        var it = openWindows.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            if (entry.getKey().tenant().equals(tenant)) {
                removedEvents += entry.getValue()[0];
                removedWindows++;
                it.remove();
            }
        }
        inFlight -= removedEvents;
        log.info("tenant EVICTED (bounded LRU) tenant={} droppedOpenWindows={} droppedInFlight={} tenantInFlight={} globalInFlight={}",
                tenant, removedWindows, removedEvents, state.inFlight(), inFlight);
    }

    private int tenantBacklog(String tenant) {
        return tenantBacklogOverrides.getOrDefault(tenant, defaultTenantBacklog);
    }

    private WindowKey windowKeyOf(Event event) {
        long start = Math.floorDiv(event.eventTimeMillis(), windowSizeMillis) * windowSizeMillis;
        return new WindowKey(event.tenant(), start, windowSizeMillis);
    }

    /**
     * 豁免判定：若接受该事件所推进的该租户水位线能关闭它自己的至少一个窗口（释放容量），
     * 则即使租户额度/全局容量已满也应放行，避免“容量满 → 无法推进水位线 → 容量永不释放”。
     * 调用方须持有锁。
     */
    private boolean wouldReleaseCapacity(TenantWatermarkTracker.TenantState ts, Event event) {
        long prospective = ts.prospectiveWatermark(event.partition(), event.eventTimeMillis());
        long horizon = prospective - watermarkTracker.allowedLatenessMillis();
        for (WindowKey k : openWindows.keySet()) {
            if (k.tenant().equals(event.tenant()) && k.windowEnd() <= horizon) {
                return true;
            }
        }
        return false;
    }

    /**
     * 只关闭指定租户自身 windowEnd + allowedLateness &lt;= 其水位线的窗口并输出结果。
     * 其他租户的窗口绝不因本租户水位线推进而关闭。调用方须持有锁。
     */
    private void finalizeTenantWindows(String tenant, TenantWatermarkTracker.TenantState ts, long watermark) {
        long horizon = watermark - watermarkTracker.allowedLatenessMillis();
        var it = openWindows.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            WindowKey key = entry.getKey();
            if (key.tenant().equals(tenant) && key.windowEnd() <= horizon) {
                long[] agg = entry.getValue();
                WindowResult result = new WindowResult(key, agg[0], agg[1], watermark);
                finalized.addLast(result);
                while (finalized.size() > MAX_RETAINED_RESULTS) {
                    finalized.removeFirst();
                }
                inFlight -= agg[0];
                ts.releaseInFlight((int) agg[0]);
                it.remove();
                log.info("window FINALIZED tenant={} window=[{},{}) count={} sum={} tenantWatermark={} tenantInFlight={} globalInFlight={}",
                        key.tenant(), key.windowStart(), key.windowEnd(), agg[0], agg[1],
                        watermark, ts.inFlight(), inFlight);
            }
        }
    }

    /** 已确定的窗口结果快照，按输出顺序返回。 */
    public List<WindowResult> finalizedResults() {
        synchronized (lock) {
            return new ArrayList<>(finalized);
        }
    }

    /** 全局观测水位线（所有租户水位线的最大值），无数据时为 Long.MIN_VALUE。 */
    public long currentWatermark() {
        synchronized (lock) {
            return watermarkTracker.globalWatermark();
        }
    }

    /** 指定租户自己的水位线，无数据时为 Long.MIN_VALUE。 */
    public long currentWatermark(String tenant) {
        synchronized (lock) {
            TenantWatermarkTracker.TenantState ts = watermarkTracker.tenantStateIfPresent(tenant);
            return ts == null ? Long.MIN_VALUE : ts.watermark();
        }
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

    /**
     * 生成处理进度快照：全局汇总 + 按租户/分区明细。整份快照在同一把锁内构建，
     * 并发写入下各计数（接受、在途、窗口数、拒绝分类）彼此自洽。
     */
    public ProgressSnapshot progressSnapshot() {
        synchronized (lock) {
            Map<String, Integer> openByTenant = new HashMap<>();
            for (WindowKey key : openWindows.keySet()) {
                openByTenant.merge(key.tenant(), 1, Integer::sum);
            }
            List<TenantProgress> tenantProgresses = new ArrayList<>();
            for (String tenant : new TreeMap<>(watermarkTracker.tenants()).keySet()) {
                TenantWatermarkTracker.TenantState ts = watermarkTracker.tenantStateIfPresent(tenant);
                List<PartitionProgress> partitions = new ArrayList<>();
                ts.partitionMaxEventTimeView().entrySet().stream()
                        .sorted(Comparator.comparingInt(Map.Entry::getKey))
                        .forEach(e -> partitions.add(new PartitionProgress(e.getKey(), e.getValue())));
                tenantProgresses.add(new TenantProgress(
                        tenant,
                        ts.watermark() == Long.MIN_VALUE ? null : ts.watermark(),
                        openByTenant.getOrDefault(tenant, 0),
                        ts.inFlight(),
                        rejectionMap(ts.rejectedByReasonView()),
                        partitions));
            }
            long gw = watermarkTracker.globalWatermark();
            return new ProgressSnapshot(
                    gw == Long.MIN_VALUE ? null : gw,
                    openWindows.size(),
                    inFlight,
                    rejectionMap(rejectedTotals.clone()),
                    tenantProgresses);
        }
    }

    private static Map<RejectionReason, Integer> rejectionMap(int[] counts) {
        Map<RejectionReason, Integer> m = new HashMap<>();
        for (RejectionReason r : RejectionReason.values()) {
            m.put(r, counts[r.ordinal()]);
        }
        return m;
    }
}
