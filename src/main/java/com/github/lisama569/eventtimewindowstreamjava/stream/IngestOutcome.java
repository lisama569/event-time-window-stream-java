package com.github.lisama569.eventtimewindowstreamjava.stream;

/**
 * 单次事件接入的判定结果。
 */
public record IngestOutcome(Status status, RejectionReason reason) {

    public enum Status {
        ACCEPTED,
        REJECTED
    }

    public static IngestOutcome accepted() {
        return new IngestOutcome(Status.ACCEPTED, null);
    }

    public static IngestOutcome rejected(RejectionReason reason) {
        return new IngestOutcome(Status.REJECTED, reason);
    }

    public boolean isAccepted() {
        return status == Status.ACCEPTED;
    }
}
