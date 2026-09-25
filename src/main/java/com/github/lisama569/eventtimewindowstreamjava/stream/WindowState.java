package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 单个窗口的聚合状态：计数与求和。只接受去重后的事件，因此天然幂等。
 */
class WindowState {
    private final WindowKey key;
    private long count;
    private long sum;

    WindowState(WindowKey key) {
        this.key = key;
    }

    void add(long value) {
        count++;
        sum += value;
    }

    WindowKey key() {
        return key;
    }

    WindowResult toResult(boolean finalized) {
        return new WindowResult(key, count, sum, finalized);
    }
}
