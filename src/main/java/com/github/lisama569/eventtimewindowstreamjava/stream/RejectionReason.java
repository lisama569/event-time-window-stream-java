package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 事件被拒绝时的可区分原因。
 */
public enum RejectionReason {
    /** 事件时间早于水位线减去允许迟到范围，超出可容忍迟到 */
    LATE_BEYOND_ALLOWED,
    /** 相同事件 ID 已被处理过，重复投递 */
    DUPLICATE,
    /** 该租户自己的在途（未定稿）事件积压达到租户级额度，仅挡该租户，其他租户不受影响 */
    TENANT_BACKLOG_FULL,
    /** 全进程在途事件数达到总量安全上限，生产端需降速（背压） */
    BACKPRESSURE_CAPACITY,
    /** 事件字段非法（如空 ID、负时间戳） */
    INVALID
}
