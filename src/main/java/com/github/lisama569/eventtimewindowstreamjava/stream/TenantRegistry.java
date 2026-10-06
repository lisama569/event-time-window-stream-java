package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * 有界租户状态注册表：
 * <ul>
 *   <li>驻留租户数受 maxTenants 限制；</li>
 *   <li>新租户到来而名额已满时回收状态：优先回收完全安静（无在途、无未定稿窗口）者；
 *       若没有安静租户，则回收距上次活动超过 idleEvictMillis 的最久未活动租户。
 *       已确定的窗口结果在回收前已输出到处理器的结果缓冲，回收不丢已定稿结果；</li>
 *   <li>没有任何可回收租户时拒绝新租户写入（按 {@link RejectionReason#TENANT_BACKLOG_FULL}）；</li>
 *   <li>全局拒绝计数在此按原因累计，原因取自有限枚举，计数结构天然有界。</li>
 * </ul>
 * 所有方法须在持有 EventStreamProcessor 锁时调用。
 */
public final class TenantRegistry {

    /** 获取租户状态的结果：成功携带状态，失败携带拒绝原因。 */
    public static final class Acquisition {
        private final TenantState state;
        private final RejectionReason rejected;

        private Acquisition(TenantState state, RejectionReason rejected) {
            this.state = state;
            this.rejected = rejected;
        }

        static Acquisition ok(TenantState state) {
            return new Acquisition(state, null);
        }

        static Acquisition rejected(RejectionReason reason) {
            return new Acquisition(null, reason);
        }

        public boolean isAccepted() {
            return state != null;
        }

        public TenantState state() {
            return state;
        }

        public RejectionReason rejection() {
            return rejected;
        }
    }

    private final int maxTenants;
    private final long idleEvictMillis;
    private final int maxPartitions;
    private final long allowedLatenessMillis;
    private final TenantQuotas quotas;
    private final LongSupplier clockMillis;
    private final Map<String, TenantState> tenants = new ConcurrentHashMap<>();
    private final Map<RejectionReason, long[]> globalRejectionCounts = new EnumMap<>(RejectionReason.class);

    TenantRegistry(int maxTenants, long idleEvictMillis, int maxPartitions,
                   long allowedLatenessMillis, TenantQuotas quotas, LongSupplier clockMillis) {
        if (maxTenants <= 0) {
            throw new IllegalArgumentException("maxTenants must be positive");
        }
        if (maxPartitions <= 0) {
            throw new IllegalArgumentException("maxPartitions must be positive");
        }
        this.maxTenants = maxTenants;
        this.idleEvictMillis = idleEvictMillis;
        this.maxPartitions = maxPartitions;
        this.allowedLatenessMillis = allowedLatenessMillis;
        this.quotas = quotas;
        this.clockMillis = clockMillis;
    }

    /** 获取已有租户状态，不触发回收/创建。 */
    TenantState get(String tenant) {
        return tenants.get(tenant);
    }

    /**
     * 获取或创建租户状态。名额满时先尝试回收安静租户；仍无名额则拒绝。
     */
    Acquisition acquire(String tenant) {
        TenantState existing = tenants.get(tenant);
        if (existing != null) {
            return Acquisition.ok(existing);
        }
        long now = clockMillis.getAsLong();
        if (tenants.size() < maxTenants) {
            return Acquisition.ok(createTenant(tenant, now));
        }
        TenantState victim = selectEvictable(now);
        if (victim == null) {
            return Acquisition.rejected(RejectionReason.TENANT_BACKLOG_FULL);
        }
        tenants.remove(victim.tenant());
        TenantState created = createTenant(tenant, now);
        return Acquisition.ok(created);
    }

    private TenantState createTenant(String tenant, long now) {
        TenantState state = new TenantState(tenant, allowedLatenessMillis, maxPartitions,
                quotas.maxInFlightFor(tenant), now);
        tenants.put(tenant, state);
        return state;
    }

    /**
     * 选出可回收租户：
     * <ul>
     *   <li>完全安静（无在途、无未定稿窗口）者随时可回收；</li>
     *   <li>否则按“放弃式 GC”：距上次活动已超过 idleEvictMillis 宽限期。
     *       每次接入都会定稿落后于水位线的全部窗口，因此长期安静租户驻留的
     *       至多是其最新事件所在的前沿窗口——它不回来推进，该窗口永远无法定稿，
     *       回收不丢任何“已可确定”的结果；它若很久以后才回来，则按新状态重新累积。</li>
     * </ul>
     * idleEvictMillis &lt;= 0 时只回收完全安静者（避免冻结时钟下误回收活跃租户）。
     */
    private TenantState selectEvictable(long now) {
        return tenants.values().stream()
                .filter(s -> s.isQuiescent()
                        || (idleEvictMillis > 0 && now - s.lastActivityMillis() >= idleEvictMillis))
                .min(Comparator.comparingLong(TenantState::lastActivityMillis))
                .orElse(null);
    }

    /** 按原因累计一条全局拒绝（同时由调用方在租户状态上累计）。 */
    void recordGlobalRejection(RejectionReason reason) {
        globalRejectionCounts.computeIfAbsent(reason, r -> new long[1])[0]++;
    }

    long globalRejectionCount(RejectionReason reason) {
        long[] c = globalRejectionCounts.get(reason);
        return c == null ? 0L : c[0];
    }

    Map<RejectionReason, long[]> globalRejectionCounts() {
        return globalRejectionCounts;
    }

    int activeTenantCount() {
        return tenants.size();
    }

    List<TenantState> snapshotStates() {
        return new ArrayList<>(tenants.values());
    }
}
