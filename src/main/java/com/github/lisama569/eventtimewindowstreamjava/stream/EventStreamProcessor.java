package com.github.lisama569.eventtimewindowstreamjava.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * 多租户进程内实时事件流处理器。
 *
 * <p>语义：
 * <ul>
 *   <li>事件按事件时间归入<b>其所属租户</b>的固定窗口 [start, start+size)。</li>
 *   <li><b>水位线按租户独立</b>：租户水位线 = 该租户各分区最大事件时间的最小值，只增不减。
 *       某个租户安静停写，只会停住它自己，不影响其他租户窗口定稿；
 *       其他租户推进也不会把本租户的正常早到数据判成太晚。</li>
 *   <li>窗口在其 windowEnd + allowedLateness &lt;= 该租户水位线时关闭并输出确定结果，
 *       此后任何属于该窗口的事件必然被该租户判为超出允许迟到范围。</li>
 *   <li>事件时间 &lt; 该租户水位线 - allowedLateness 的事件以 LATE_BEYOND_ALLOWED 拒绝。</li>
 *   <li>相同 eventId 重复投递以 DUPLICATE 拒绝，幂等不重复计数；去重记录有界。</li>
 *   <li><b>租户级额度</b>：某租户在途（已定稿前积压）事件达到它自己的额度时，
 *       仅以 TENANT_BACKLOG_FULL 挡下该租户，其他租户照常读写；
 *       被拒事件不记录去重，租户降速重投不会误判为重复。</li>
 *   <li>另有全进程在途总量安全上限 BACKPRESSURE_CAPACITY（默认远高于单租户额度，
 *       仅作进程内存保护）。</li>
 *   <li>能推进水位线关闭窗口从而释放容量的事件豁免容量拒绝，避免容量死锁。</li>
 *   <li>所有计数有界：租户数（maxTenants，安静租户可回收）、每租户分区数
 *       （maxPartitionsPerTenant）、去重条目（maxDedupEntries）、结果缓冲（1024）、
 *       拒绝原因（有限枚举）。</li>
 * </ul>
 * 线程安全：所有状态迁移在同一把锁内完成，支持多租户多分区并发写入。
 */
public class EventStreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(EventStreamProcessor.class);
    private static final int MAX_RETAINED_RESULTS = 1024;
    private static final int DEFAULT_MAX_TENANTS = 128;
    private static final int DEFAULT_MAX_PARTITIONS_PER_TENANT = 1_024;
    private static final long DEFAULT_IDLE_EVICT_MILLIS = 60_000L;
    private static final int DEFAULT_GLOBAL_INFLIGHT = 1_000_000;

    private final long windowSizeMillis;
    private final int globalMaxInFlight;
    private final DedupStore dedupStore;
    private final TenantRegistry registry;
    private final LongSupplier clockMillis;

    private final Object lock = new Object();
    private final Deque<WindowResult> finalized = new ArrayDeque<>();
    private int globalInFlight;

    public EventStreamProcessor(long windowSizeMillis, long allowedLatenessMillis,
                                int maxInFlight, int maxDedupEntries) {
        this(windowSizeMillis, allowedLatenessMillis,
                new TenantQuotas(maxInFlight, Map.of()),
                DEFAULT_GLOBAL_INFLIGHT, maxDedupEntries,
                DEFAULT_MAX_TENANTS, DEFAULT_MAX_PARTITIONS_PER_TENANT,
                DEFAULT_IDLE_EVICT_MILLIS, System::currentTimeMillis);
    }

    public EventStreamProcessor(long windowSizeMillis, long allowedLatenessMillis,
                                TenantQuotas tenantQuotas, int globalMaxInFlight,
                                int maxDedupEntries, int maxTenants,
                                int maxPartitionsPerTenant, long idleTenantEvictMillis,
                                LongSupplier clockMillis) {
        this.windowSizeMillis = windowSizeMillis;
        this.globalMaxInFlight = globalMaxInFlight;
        this.dedupStore = new DedupStore(maxDedupEntries);
        this.clockMillis = clockMillis;
        this.registry = new TenantRegistry(maxTenants, idleTenantEvictMillis,
                maxPartitionsPerTenant, allowedLatenessMillis, tenantQuotas, clockMillis);
    }

    /** 接入一条事件，返回可区分原因的判定结果。 */
    public IngestOutcome ingest(Event event) {
        if (event == null || event.eventId() == null || event.eventId().isBlank()
                || event.tenant() == null || event.tenant().isBlank()
                || event.eventTimeMillis() < 0) {
            log.info("ingest REJECTED reason=INVALID event={}", event);
            return IngestOutcome.rejected(RejectionReason.INVALID);
        }
        synchronized (lock) {
            TenantRegistry.Acquisition acquisition = registry.acquire(event.tenant());
            if (!acquisition.isAccepted()) {
                return reject(event, null, Long.MIN_VALUE, acquisition.rejection());
            }
            TenantState ts = acquisition.state();
            WatermarkTracker wt = ts.watermarkTracker();
            long wm = wt.currentWatermark();

            if (wt.isBeyondAllowedLateness(event.eventTimeMillis())) {
                return reject(event, ts, wm, RejectionReason.LATE_BEYOND_ALLOWED);
            }
            if (dedupStore.contains(event.eventId())) {
                return reject(event, ts, wm, RejectionReason.DUPLICATE);
            }
            if (!ts.registerPartition(event.partition())) {
                return reject(event, ts, wm, RejectionReason.INVALID);
            }

            boolean releasesTenantCapacity = wouldReleaseCapacity(ts, event);
            if (ts.inFlight() >= ts.quota() && !releasesTenantCapacity) {
                return reject(event, ts, wm, RejectionReason.TENANT_BACKLOG_FULL);
            }
            if (globalInFlight >= globalMaxInFlight && !releasesTenantCapacity) {
                return reject(event, ts, wm, RejectionReason.BACKPRESSURE_CAPACITY);
            }

            dedupStore.markIfAbsent(event.eventId());
            long newWatermark = wt.observe(event.partition(), event.eventTimeMillis());
            WindowKey key = windowKeyOf(event);
            ts.openWindows().computeIfAbsent(key, k -> new long[2]);
            long[] agg = ts.openWindows().get(key);
            agg[0] += 1;
            agg[1] += event.amount();
            ts.incrementInFlight();
            globalInFlight++;
            ts.noteAccepted(clockMillis.getAsLong());
            log.info("ingest ACCEPTED id={} tenant={} partition={} eventTime={} window=[{},{}) "
                            + "tenantWatermark={}->{} tenantInFlight={}/{} globalInFlight={}/{}",
                    event.eventId(), event.tenant(), event.partition(), event.eventTimeMillis(),
                    key.windowStart(), key.windowEnd(), wm, newWatermark,
                    ts.inFlight(), ts.quota(), globalInFlight, globalMaxInFlight);

            finalizeWindows(ts, newWatermark);
            return IngestOutcome.accepted();
        }
    }

    /** 记录租户与全局拒绝计数并输出可区分原因的日志。调用方须持有锁。 */
    private IngestOutcome reject(Event event, TenantState ts, long watermark, RejectionReason reason) {
        if (ts != null) {
            ts.recordRejection(reason);
        }
        registry.recordGlobalRejection(reason);
        log.info("ingest REJECTED reason={} id={} tenant={} partition={} eventTime={} watermark={}"
                        + (reason == RejectionReason.TENANT_BACKLOG_FULL
                                ? " tenantInFlight={} tenantQuota={}" : ""),
                reason, event == null ? null : event.eventId(),
                event == null ? null : event.tenant(),
                event == null ? null : event.partition(),
                event == null ? null : event.eventTimeMillis(), watermark,
                ts == null ? null : ts.inFlight(), ts == null ? null : ts.quota());
        return IngestOutcome.rejected(reason);
    }

    private WindowKey windowKeyOf(Event event) {
        long start = Math.floorDiv(event.eventTimeMillis(), windowSizeMillis) * windowSizeMillis;
        return new WindowKey(event.tenant(), start, windowSizeMillis);
    }

    /**
     * 容量豁免：若接受该事件所推进的<b>该租户</b>水位线能关闭其至少一个窗口（释放容量），
     * 即使该租户额度（或全进程总量）已满也放行，避免“容量满 → 无法推进水位线 →
     * 容量永不释放”的死锁。调用方须持有锁。
     */
    private boolean wouldReleaseCapacity(TenantState ts, Event event) {
        WatermarkTracker wt = ts.watermarkTracker();
        long prospective = wt.prospectiveWatermark(event.partition(), event.eventTimeMillis());
        long horizon = prospective - wt.allowedLatenessMillis();
        return ts.openWindows().keySet().stream().anyMatch(k -> k.windowEnd() <= horizon);
    }

    /** 关闭该租户所有 windowEnd + allowedLateness &lt;= 其水位线的窗口并输出结果。调用方须持有锁。 */
    private void finalizeWindows(TenantState ts, long watermark) {
        long horizon = watermark - ts.watermarkTracker().allowedLatenessMillis();
        var it = ts.openWindows().entrySet().iterator();
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
                ts.releaseInFlight((int) agg[0]);
                globalInFlight -= (int) agg[0];
                ts.noteWindowFinalized(clockMillis.getAsLong());
                it.remove();
                log.info("window FINALIZED tenant={} window=[{},{}) count={} sum={} "
                                + "tenantWatermark={} tenantInFlight={}/{} globalInFlight={}",
                        key.tenant(), key.windowStart(), key.windowEnd(), agg[0], agg[1],
                        watermark, ts.inFlight(), ts.quota(), globalInFlight);
            }
        }
    }

    /** 已确定的窗口结果快照，按输出顺序返回。 */
    public List<WindowResult> finalizedResults() {
        synchronized (lock) {
            return new ArrayList<>(finalized);
        }
    }

    /** 全进程在途事件总数。 */
    public int inFlightCount() {
        synchronized (lock) {
            return globalInFlight;
        }
    }

    /** 全进程未定稿窗口总数。 */
    public int openWindowCount() {
        synchronized (lock) {
            int n = 0;
            for (TenantState ts : registry.snapshotStates()) {
                n += ts.openWindows().size();
            }
            return n;
        }
    }

    /** 全局水位线：取各活跃租户水位线的最小值（仅观测用）。无租户时为 Long.MIN_VALUE。 */
    public long currentWatermark() {
        synchronized (lock) {
            long min = Long.MAX_VALUE;
            for (TenantState ts : registry.snapshotStates()) {
                min = Math.min(min, ts.watermarkTracker().currentWatermark());
            }
            return min == Long.MAX_VALUE ? Long.MIN_VALUE : min;
        }
    }

    /** 指定租户当前水位线（无该租户状态时为 Long.MIN_VALUE）。 */
    public long tenantWatermark(String tenant) {
        synchronized (lock) {
            TenantState ts = registry.get(tenant);
            return ts == null ? Long.MIN_VALUE : ts.watermarkTracker().currentWatermark();
        }
    }

    /** 指定租户当前在途（未定稿积压）事件数。 */
    public int tenantInFlight(String tenant) {
        synchronized (lock) {
            TenantState ts = registry.get(tenant);
            return ts == null ? 0 : ts.inFlight();
        }
    }

    /** 指定租户额度。无驻留状态时返回配置中的额度。 */
    public int tenantQuota(String tenant) {
        synchronized (lock) {
            TenantState ts = registry.get(tenant);
            return ts == null ? -1 : ts.quota();
        }
    }

    /** 全局累计的按原因拒绝条数。 */
    public Map<RejectionReason, Long> globalRejectionCounts() {
        synchronized (lock) {
            Map<RejectionReason, Long> out = new LinkedHashMap<>();
            for (RejectionReason r : RejectionReason.values()) {
                out.put(r, registry.globalRejectionCount(r));
            }
            return out;
        }
    }

    /**
     * 生成处理进度快照：按租户/分区给出推进事件时间、未定稿窗口数与按原因分类的拒绝条数。
     * 在同一把锁内复制全部计数，多线程并发写入下返回数字自洽。
     */
    public ProgressSnapshot progressSnapshot() {
        return progressSnapshot(null);
    }

    /** 生成进度快照，tenant 非空时只包含指定租户。 */
    public ProgressSnapshot progressSnapshot(String tenantFilter) {
        synchronized (lock) {
            Map<String, Long> globalRejects = new LinkedHashMap<>();
            for (RejectionReason r : RejectionReason.values()) {
                globalRejects.put(r.name(), registry.globalRejectionCount(r));
            }

            List<ProgressSnapshot.TenantProgress> tenantProgresses = new ArrayList<>();
            int totalInFlight = 0;
            for (TenantState ts : registry.snapshotStates()) {
                if (tenantFilter != null && !tenantFilter.isBlank()
                        && !tenantFilter.equals(ts.tenant())) {
                    continue;
                }
                totalInFlight += ts.inFlight();
                tenantProgresses.add(toTenantProgress(ts));
            }
            tenantProgresses.sort((a, b) -> a.tenant().compareTo(b.tenant()));

            long globalWm = Long.MAX_VALUE;
            for (ProgressSnapshot.TenantProgress tp : tenantProgresses) {
                globalWm = Math.min(globalWm, tp.watermark());
            }
            long watermark = globalWm == Long.MAX_VALUE ? Long.MIN_VALUE : globalWm;
            return new ProgressSnapshot(watermark, totalInFlight,
                    tenantFilter == null || tenantFilter.isBlank()
                            ? registry.activeTenantCount() : tenantProgresses.size(),
                    Map.copyOf(globalRejects), List.copyOf(tenantProgresses));
        }
    }

    private ProgressSnapshot.TenantProgress toTenantProgress(TenantState ts) {
        List<ProgressSnapshot.PartitionProgress> partitions = new ArrayList<>();
        for (var entry : ts.watermarkTracker().partitionMaxEventTimes().entrySet()) {
            partitions.add(new ProgressSnapshot.PartitionProgress(entry.getKey(), entry.getValue()));
        }
        partitions.sort((a, b) -> Integer.compare(a.partition(), b.partition()));

        Map<String, Long> rejects = new LinkedHashMap<>();
        for (RejectionReason r : RejectionReason.values()) {
            rejects.put(r.name(), ts.rejectionCount(r));
        }
        return new ProgressSnapshot.TenantProgress(
                ts.tenant(),
                ts.watermarkTracker().currentWatermark(),
                ts.inFlight(),
                ts.quota(),
                ts.openWindows().size(),
                List.copyOf(partitions),
                Map.copyOf(rejects));
    }
}
