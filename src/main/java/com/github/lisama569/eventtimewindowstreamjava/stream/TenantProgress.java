package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.List;
import java.util.Map;

/**
 * 单个租户的处理进度快照：当前租户水位线、未定稿窗口数、在途事件数、
 * 按原因分类的拒绝条数，以及其各分区推进位置。
 *
 * @param tenant            租户标识
 * @param watermarkMillis   租户水位线（该租户各分区最大事件时间的最小值）；尚无数据时为 null
 * @param openWindowCount   未定稿窗口数
 * @param inFlight          该租户未确定（未定稿窗口内）的在途事件数
 * @param rejectedByReason  按拒绝原因分类的累计拒绝条数
 * @param partitions        各分区已见最大事件时间
 */
public record TenantProgress(String tenant,
                             Long watermarkMillis,
                             int openWindowCount,
                             int inFlight,
                             Map<RejectionReason, Integer> rejectedByReason,
                             List<PartitionProgress> partitions) {
}
