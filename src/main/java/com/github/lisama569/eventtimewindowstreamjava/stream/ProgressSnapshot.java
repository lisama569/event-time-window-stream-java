package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.List;
import java.util.Map;

/**
 * 处理进度快照骨架（实现待填充）：按租户/分区给出推进位置、
 * 未定稿窗口数与按原因分类的拒绝条数。
 */
public record ProgressSnapshot(long globalWatermark, int totalInFlight, int activeTenants,
                               Map<String, Long> globalRejectionCounts,
                               List<TenantProgress> tenants) {

    /** 单个分区推进位置。 */
    public record PartitionProgress(int partition, long maxEventTime) {
    }

    /** 单个租户的进度。 */
    public record TenantProgress(String tenant, long watermark, int inFlight, int quota,
                                 int openWindowCount,
                                 List<PartitionProgress> partitions,
                                 Map<String, Long> rejectionCounts) {
    }
}
