package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 单个 (租户, 分区) 的处理进度快照。
 *
 * @param partition         分区标识
 * @param maxEventTimeMillis 该分区已见最大事件时间；尚无数据时为 null
 */
public record PartitionProgress(int partition, Long maxEventTimeMillis) {
}
