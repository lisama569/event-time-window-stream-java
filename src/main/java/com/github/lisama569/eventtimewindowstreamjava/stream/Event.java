package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 一条待处理事件。事件时间（eventTimeMillis）决定其归属的窗口，
 * 与到达处理系统的时刻无关。
 *
 * @param eventId        全局唯一事件 ID，用于幂等去重
 * @param tenant         租户标识，不同租户状态相互隔离
 * @param partition      分区标识，水位线按分区独立推进
 * @param eventTimeMillis 事件时间（epoch 毫秒）
 * @param amount         待聚合的数值载荷
 */
public record Event(String eventId, String tenant, int partition, long eventTimeMillis, long amount) {
}
