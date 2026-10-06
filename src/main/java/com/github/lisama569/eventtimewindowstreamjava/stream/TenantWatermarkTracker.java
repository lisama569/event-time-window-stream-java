package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按 (租户, 分区) 跟踪已见最大事件时间的水位线组件。
 *
 * <p>水位线语义（租户级）：
 * <ul>
 *   <li>每个分区独立记录本租户在该分区上的已见最大事件时间；</li>
 *   <li><b>租户水位线 = 该租户各分区最大事件时间的最小值</b>，只增不减；</li>
 *   <li>租户之间互不约束：安静（长时间无数据）的租户不会卡住其他租户的窗口关闭，
 *       推进快的租户也不会把其他租户的正常数据判成太晚。</li>
 * </ul>
 *
 * <p>有界性：租户数与每租户分区数均有上限。
 * 租户数达到上限且出现新租户时，按最近访问顺序淘汰最久没有任何写入的租户（LRU）——
 * 仍在活跃写入的租户总是最近被访问，不会被淘汰；被淘汰租户残留的未定稿窗口由
 * EventStreamProcessor 负责清理并同步全局在途计数，故全局状态始终自洽。
 * 被淘汰租户若之后再写入，按“重新开始”处理；每租户分区数超限时按最早插入顺序
 * 淘汰最旧分区（只会抬高分区最小值，水位线保持单调不减）。
 *
 * <p>线程安全：所有方法都须在 EventStreamProcessor 的同一把锁内调用。
 */
public final class TenantWatermarkTracker {

    private static final Logger log = LoggerFactory.getLogger(TenantWatermarkTracker.class);

    /** 单个租户的水位线、在途计数与拒绝计数状态。 */
    public static final class TenantState {
        private final int maxPartitions;
        private final LinkedHashMap<Integer, Long> maxEventTimePerPartition;
        private final int[] rejectedByReason;
        private long watermark = Long.MIN_VALUE;
        private int inFlight;

        private TenantState(int maxPartitions) {
            this.maxPartitions = maxPartitions;
            this.maxEventTimePerPartition = new LinkedHashMap<>(16, 0.75f, false);
            this.rejectedByReason = new int[RejectionReason.values().length];
        }

        /**
         * 用一条已接受事件的事件时间推进对应分区，并在超容量时淘汰最旧分区，
         * 返回推进后的租户水位线（只增不减）。
         */
        long observe(int partition, long eventTimeMillis) {
            Long cur = maxEventTimePerPartition.get(partition);
            if (cur == null) {
                if (maxEventTimePerPartition.size() >= maxPartitions) {
                    evictEldestPartition();
                }
                maxEventTimePerPartition.put(partition, eventTimeMillis);
            } else if (eventTimeMillis > cur) {
                maxEventTimePerPartition.put(partition, eventTimeMillis);
            }
            recomputeWatermark();
            return watermark;
        }

        private void evictEldestPartition() {
            var it = maxEventTimePerPartition.keySet().iterator();
            if (it.hasNext()) {
                Integer eldest = it.next();
                it.remove();
                log.info("partition tracker EVICTED (bounded) partition={}", eldest);
            }
        }

        private void recomputeWatermark() {
            if (maxEventTimePerPartition.isEmpty()) {
                return;
            }
            long min = Long.MAX_VALUE;
            for (long v : maxEventTimePerPartition.values()) {
                min = Math.min(min, v);
            }
            if (min > watermark) {
                watermark = min;
            }
        }

        /** 假设用该事件时间推进对应分区后的租户水位线（无副作用，供豁免判定使用）。 */
        long prospectiveWatermark(int partition, long eventTimeMillis) {
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
            return Math.max(watermark, min == Long.MAX_VALUE ? Long.MIN_VALUE : min);
        }

        /** eventTime &lt; 租户水位线 - allowedLateness 视为不可容忍的迟到。 */
        boolean isBeyondAllowedLateness(long eventTimeMillis, long allowedLatenessMillis) {
            return watermark != Long.MIN_VALUE && eventTimeMillis < watermark - allowedLatenessMillis;
        }

        long watermark() {
            return watermark;
        }

        int inFlight() {
            return inFlight;
        }

        void incrementInFlight() {
            inFlight++;
        }

        void releaseInFlight(int n) {
            inFlight -= n;
        }

        void recordRejection(RejectionReason reason) {
            rejectedByReason[reason.ordinal()]++;
        }

        int[] rejectedByReasonView() {
            return rejectedByReason.clone();
        }

        /** 分区 -> 已见最大事件时间的快照（按首次插入顺序）。 */
        Map<Integer, Long> partitionMaxEventTimeView() {
            return new LinkedHashMap<>(maxEventTimePerPartition);
        }
    }

    private final long allowedLatenessMillis;
    private final int maxTenants;
    private final int maxPartitionsPerTenant;
    /** 按访问顺序排列：每次 get/put 已存在键都会把它移到最新端，最旧端即最久未写入的租户。 */
    private final LinkedHashMap<String, TenantState> tenants;

    public TenantWatermarkTracker(long allowedLatenessMillis, int maxTenants, int maxPartitionsPerTenant) {
        if (maxTenants <= 0 || maxPartitionsPerTenant <= 0) {
            throw new IllegalArgumentException("bounds must be positive");
        }
        this.allowedLatenessMillis = allowedLatenessMillis;
        this.maxTenants = maxTenants;
        this.maxPartitionsPerTenant = maxPartitionsPerTenant;
        this.tenants = new LinkedHashMap<>(16, 0.75f, true);
    }

    public long allowedLatenessMillis() {
        return allowedLatenessMillis;
    }

    /** 取得租户状态（写入路径）：不存在则创建；已存在则按 LRU 刷新访问顺序。 */
    public TenantState tenantState(String tenant) {
        TenantState s = tenants.get(tenant);
        if (s == null) {
            s = new TenantState(maxPartitionsPerTenant);
            tenants.put(tenant, s);
        }
        return s;
    }

    /** 租户数是否已达上限（调用方须据此先淘汰再创建）。 */
    public boolean atCapacity() {
        return tenants.size() >= maxTenants;
    }

    /**
     * 淘汰最久没有任何写入的租户（LRU 最旧端），返回被淘汰者；
     * 调用方负责清理该租户残留的未定稿窗口与全局在途计数。
     */
    public Map.Entry<String, TenantState> evictEldestTenant() {
        var it = tenants.entrySet().iterator();
        if (!it.hasNext()) {
            return null;
        }
        Map.Entry<String, TenantState> victim = it.next();
        it.remove();
        return victim;
    }

    /** 取得租户状态（只读路径），不存在返回 null；不改变 LRU 顺序。 */
    public TenantState tenantStateIfPresent(String tenant) {
        for (var entry : tenants.entrySet()) {
            if (entry.getKey().equals(tenant)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /** 返回不可变视图，供快照构建遍历。 */
    public Map<String, TenantState> tenants() {
        return Collections.unmodifiableMap(tenants);
    }

    /** 所有租户水位线的最大值（兼容旧的“全局水位线”观测口径；无租户时为 MIN_VALUE）。 */
    public long globalWatermark() {
        long max = Long.MIN_VALUE;
        for (TenantState s : tenants.values()) {
            max = Math.max(max, s.watermark());
        }
        return max;
    }
}
