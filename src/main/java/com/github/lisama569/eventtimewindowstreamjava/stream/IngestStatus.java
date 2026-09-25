package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 事件摄入结果的可区分状态。
 */
public enum IngestStatus {
    /** 正常计入窗口 */
    ACCEPTED,
    /** 重复投递，幂等忽略 */
    DUPLICATE,
    /** 事件时间早于水位线允许迟到范围，拒绝并单独归类 */
    REJECTED_LATE,
    /** 处理能力不足，背压拒绝 */
    REJECTED_BACKPRESSURE
}
