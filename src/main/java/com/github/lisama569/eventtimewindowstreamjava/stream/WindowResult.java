package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 窗口关闭（水位线越过窗口右边界）后输出的确定结果。
 */
public record WindowResult(WindowKey key, long count, long sum, long emittedAtWatermark) {
}
