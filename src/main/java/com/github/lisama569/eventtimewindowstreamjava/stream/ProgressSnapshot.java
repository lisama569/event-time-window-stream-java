package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.List;
import java.util.Map;

/**
 * 全量处理进度快照：全局汇总 + 按租户明细。整个快照在同一把锁内生成，
 * 多租户并发写入下各字段相互自洽。
 *
 * @param globalWatermarkMillis 所有租户水位线中的最大值（向后兼容的全局观测口径）；无租户时为 null
 * @param openWindowCount       全部租户未定稿窗口总数
 * @param inFlight              全部租户在途事件总数
 * @param rejectedByReason      全局按拒绝原因分类的累计拒绝条数
 * @param tenants               按租户标识排序的明细
 */
public record ProgressSnapshot(Long globalWatermarkMillis,
                               int openWindowCount,
                               int inFlight,
                               Map<RejectionReason, Integer> rejectedByReason,
                               List<TenantProgress> tenants) {
}
