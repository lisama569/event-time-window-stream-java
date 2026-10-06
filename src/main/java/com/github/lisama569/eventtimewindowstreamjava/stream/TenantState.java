package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * 单个租户的全部处理状态：
 * <ul>
 *   <li><b>独立水位线</b>：水位线只在该租户自己的分区之间取最小值，
 *       其他租户推进或停写都不影响本租户的迟到判定与窗口定稿；</li>
 *   <li><b>独立未定稿窗口与积压计数</b>：租户额度按 {@link #inFlight} 独立计量；</li>
 *   <li><b>独立拒绝计数</b>：按 {@link RejectionReason} 分类，仅在本租户状态存活期累积；</li>
 *   <li><b>有界</b>：分区数受 maxPartitions 限制，拒绝原因枚举天然有限。</li>
 * </ul>
 * 本对象的可变状态不自带同步，所有迁移须在持有 EventStreamProcessor 锁时进行。
 */
public final class TenantState {

    private final String tenant;
    private final int quota;
    private final int maxPartitions;
    private final WatermarkTracker watermarkTracker;
    private final Map<WindowKey, long[]> openWindows = new HashMap<>();
    private final Map<RejectionReason, long[]> rejectionCounts = new EnumMap<>(RejectionReason.class);

    private int inFlight;
    private long acceptedCount;
    private long finalizedWindowCount;
    private long lastActivityMillis;

    TenantState(String tenant, long allowedLatenessMillis, int maxPartitions,
                int quota, long nowMillis) {
        this.tenant = tenant;
        this.maxPartitions = maxPartitions;
        this.quota = quota;
        this.watermarkTracker = new WatermarkTracker(allowedLatenessMillis);
        this.lastActivityMillis = nowMillis;
    }

    String tenant() {
        return tenant;
    }

    WatermarkTracker watermarkTracker() {
        return watermarkTracker;
    }

    Map<WindowKey, long[]> openWindows() {
        return openWindows;
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

    int quota() {
        return quota;
    }

    long acceptedCount() {
        return acceptedCount;
    }

    long finalizedWindowCount() {
        return finalizedWindowCount;
    }

    void noteAccepted(long nowMillis) {
        acceptedCount++;
        lastActivityMillis = nowMillis;
    }

    long lastActivityMillis() {
        return lastActivityMillis;
    }

    /** 安静：没有任何未定稿窗口/在途事件，回收后不会丢失尚未输出的结果。 */
    boolean isQuiescent() {
        return inFlight == 0 && openWindows.isEmpty();
    }

    /**
     * 注册分区（仅首次出现时新增）。分区数已有界；新分区将超出上限时拒绝并返回 false。
     * 调用方须持有处理器锁。
     */
    boolean registerPartition(int partition) {
        if (!watermarkTracker.knowsPartition(partition)
                && watermarkTracker.partitionCount() >= maxPartitions) {
            return false;
        }
        return true;
    }

    /** 按原因累计一条拒绝（该原因计数器延迟创建，枚举种类有限，计数结构有界）。 */
    void recordRejection(RejectionReason reason) {
        rejectionCounts.computeIfAbsent(reason, r -> new long[1])[0]++;
    }

    Map<RejectionReason, long[]> rejectionCounts() {
        return Collections.unmodifiableMap(rejectionCounts);
    }

    long rejectionCount(RejectionReason reason) {
        long[] c = rejectionCounts.get(reason);
        return c == null ? 0L : c[0];
    }

    void noteWindowFinalized(long finalizedAtMillis) {
        finalizedWindowCount++;
        lastActivityMillis = finalizedAtMillis;
    }
}
