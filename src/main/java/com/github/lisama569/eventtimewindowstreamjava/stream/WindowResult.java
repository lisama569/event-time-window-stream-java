package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 窗口聚合结果。finalized 表示水位线已越过窗口边界、结果确定。
 */
public record WindowResult(WindowKey key, long count, long sum, boolean finalized) {
}
