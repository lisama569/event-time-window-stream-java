package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.concurrent.Semaphore;

/**
 * 生产端入口：用有界并发许可实现背压。
 *
 * <p>许可耗尽时不阻塞、不排队，立即返回 REJECTED_BACKPRESSURE，
 * 给出明确的拒绝语义，避免长期运行下出现无界缓冲。
 */
public class IngestGateway {
    private final EventStreamProcessor processor;
    private final Semaphore permits;

    public IngestGateway(EventStreamProcessor processor, int maxConcurrent) {
        this.processor = processor;
        this.permits = new Semaphore(maxConcurrent);
    }

    public IngestResult submit(Event event) {
        if (!permits.tryAcquire()) {
            return IngestResult.of(IngestStatus.REJECTED_BACKPRESSURE,
                    "并发许可耗尽，处理能力不足，拒绝摄入 eventId=%s".formatted(event.eventId()));
        }
        try {
            return processor.ingest(event);
        } finally {
            permits.release();
        }
    }

    public int availablePermits() {
        return permits.availablePermits();
    }
}
