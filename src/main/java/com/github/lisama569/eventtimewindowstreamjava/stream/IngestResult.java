package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 摄入结果：状态 + 可解释的判定依据。
 */
public record IngestResult(IngestStatus status, String reason) {
    public static IngestResult of(IngestStatus status, String reason) {
        return new IngestResult(status, reason);
    }
}
