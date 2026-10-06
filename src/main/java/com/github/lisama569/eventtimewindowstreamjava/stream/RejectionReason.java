package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 事件被拒绝时的可区分原因。
 */
public enum RejectionReason {
    /** 事件时间早于水位线减去允许迟到范围，超出可容忍迟到 */
    LATE_BEYOND_ALLOWED,
    /** 相同事件 ID 已被处理过，重复投递 */
    DUPLICATE,
    /** 在途事件数达到容量上限，生产端需降速（背压） */
    BACKPRESSURE_CAPACITY,
    /** 该租户自身的未确定在途事件数达到租户额度，只挡该租户，不影响其他租户 */
    TENANT_BACKLOG_LIMIT,
    /** 事件字段非法（如空 ID、负时间戳） */
    INVALID
}
