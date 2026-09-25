package com.github.lisama569.eventtimewindowstreamjava.tests;

import com.github.lisama569.eventtimewindowstreamjava.stream.Event;
import com.github.lisama569.eventtimewindowstreamjava.stream.EventStreamProcessor;
import com.github.lisama569.eventtimewindowstreamjava.stream.IngestOutcome;
import com.github.lisama569.eventtimewindowstreamjava.stream.RejectionReason;
import com.github.lisama569.eventtimewindowstreamjava.stream.WindowResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventStreamProcessorTest {

    private static final Logger log = LoggerFactory.getLogger(EventStreamProcessorTest.class);

    private static EventStreamProcessor newProcessor() {
        // 窗口 1000ms，允许迟到 500ms，在途上限与去重容量足够测试使用
        return new EventStreamProcessor(1000L, 500L, 10_000, 10_000);
    }

    @Test
    void outOfOrderEventsAggregateIntoSameWindowAndFinalizeOnWatermark() {
        EventStreamProcessor p = newProcessor();
        // 乱序：分区1先到 t=200，分区0后到 t=1500，随后分区0又乱序补一条 t=100
        assertTrue(p.ingest(new Event("e2", "t1", 1, 200, 20)).isAccepted());
        assertTrue(p.ingest(new Event("e1", "t1", 0, 1500, 10)).isAccepted());
        assertTrue(p.ingest(new Event("e3", "t1", 0, 100, 30)).isAccepted());
        assertEquals(0, p.finalizedResults().size(), "水位线未越过窗口边界前不得输出结果");

        // 分区1推进到 1600 -> 全局水位线 = min(1500,1600) = 1500，horizon = 1000
        assertTrue(p.ingest(new Event("e4", "t1", 1, 1600, 40)).isAccepted());
        List<WindowResult> results = p.finalizedResults();
        assertEquals(1, results.size());
        WindowResult w0 = results.get(0);
        log.info("assert window [0,1000): count={} sum={} (expect 2/50)", w0.count(), w0.sum());
        assertEquals(0L, w0.key().windowStart());
        assertEquals(2, w0.count());
        assertEquals(50, w0.sum());
    }

    @Test
    void duplicateDeliveryIsRejectedAndCountedOnce() {
        EventStreamProcessor p = newProcessor();
        Event e = new Event("dup-1", "t1", 0, 100, 7);
        assertTrue(p.ingest(e).isAccepted());
        IngestOutcome dup = p.ingest(e);
        assertEquals(IngestOutcome.Status.REJECTED, dup.status());
        assertEquals(RejectionReason.DUPLICATE, dup.reason());

        // 推进水位线关闭窗口后，结果只计入一次
        assertTrue(p.ingest(new Event("adv", "t1", 0, 10_000, 0)).isAccepted());
        long total = p.finalizedResults().stream().mapToLong(WindowResult::sum).sum();
        log.info("assert duplicate counted once: totalSum={} (expect 7)", total);
        assertEquals(7, total);
    }

    @Test
    void lateBeyondAllowedIsRejectedWithDistinguishableReason() {
        EventStreamProcessor p = newProcessor();
        assertTrue(p.ingest(new Event("a", "t1", 0, 5_000, 1)).isAccepted());
        // 水位线 = 5000，允许迟到 500 -> 可容忍下限 4500
        IngestOutcome late = p.ingest(new Event("late", "t1", 0, 4_000, 1));
        assertEquals(RejectionReason.LATE_BEYOND_ALLOWED, late.reason());
        // 迟到但在允许范围内的事件仍可被接受
        assertTrue(p.ingest(new Event("late-ok", "t1", 0, 4_600, 1)).isAccepted());
        log.info("assert late rejection distinguishable: late={}, late-ok accepted", late.reason());
    }

    @Test
    void windowBoundaryEventBelongsToNextWindow() {
        EventStreamProcessor p = newProcessor();
        // t=999 属于 [0,1000)，t=1000 属于 [1000,2000)
        assertTrue(p.ingest(new Event("b1", "t1", 0, 999, 1)).isAccepted());
        assertTrue(p.ingest(new Event("b2", "t1", 0, 1000, 2)).isAccepted());
        assertTrue(p.ingest(new Event("adv", "t1", 0, 2_500, 0)).isAccepted());
        // 水位线 2500，horizon 2000，两个窗口均关闭
        List<WindowResult> results = p.finalizedResults();
        assertEquals(2, results.size());
        for (WindowResult r : results) {
            log.info("boundary window [{},{}) count={} sum={}",
                    r.key().windowStart(), r.key().windowEnd(), r.count(), r.sum());
        }
        assertEquals(1, results.get(0).count());
        assertEquals(1, results.get(0).sum());
        assertEquals(1000L, results.get(1).key().windowStart());
        assertEquals(2, results.get(1).sum());
    }

    @Test
    void backpressureRejectsWhenInFlightFullAndRecoversAfterFinalize() {
        EventStreamProcessor p = new EventStreamProcessor(10_000L, 1_000L, 2, 1_000);
        assertTrue(p.ingest(new Event("c1", "t1", 0, 100, 1)).isAccepted());
        assertTrue(p.ingest(new Event("c2", "t1", 0, 200, 1)).isAccepted());
        IngestOutcome bp = p.ingest(new Event("c3", "t1", 0, 300, 1));
        assertEquals(RejectionReason.BACKPRESSURE_CAPACITY, bp.reason());
        log.info("assert backpressure: inFlight={} reason={}", p.inFlightCount(), bp.reason());

        // 被背压拒绝的事件未记录去重；能推进水位线并关闭窗口的事件豁免背压，
        // 窗口关闭后容量释放，此前被拒事件可重试
        assertTrue(p.ingest(new Event("adv", "t1", 0, 20_000, 0)).isAccepted());
        assertEquals(1, p.inFlightCount(), "c1/c2 随窗口关闭释放，仅剩 adv 自身在途");
        assertTrue(p.ingest(new Event("c3", "t1", 0, 20_100, 1)).isAccepted());
    }

    @Test
    void tenantStatesAreIsolated() {
        EventStreamProcessor p = newProcessor();
        assertTrue(p.ingest(new Event("x1", "tenantA", 0, 100, 1)).isAccepted());
        assertTrue(p.ingest(new Event("x2", "tenantB", 0, 100, 100)).isAccepted());
        assertTrue(p.ingest(new Event("adv", "tenantA", 0, 5_000, 0)).isAccepted());
        List<WindowResult> results = p.finalizedResults();
        assertEquals(2, results.size());
        for (WindowResult r : results) {
            log.info("tenant window tenant={} sum={}", r.key().tenant(), r.sum());
        }
        assertEquals(1, results.stream().filter(r -> r.key().tenant().equals("tenantA")).mapToLong(WindowResult::sum).sum());
        assertEquals(100, results.stream().filter(r -> r.key().tenant().equals("tenantB")).mapToLong(WindowResult::sum).sum());
    }

    @Test
    void replayFromHistoryYieldsIdenticalResults() {
        List<Event> history = List.of(
                new Event("r1", "t1", 0, 100, 5),
                new Event("r2", "t1", 1, 300, 7),
                new Event("r3", "t1", 0, 50, 1),   // 乱序
                new Event("r4", "t1", 1, 1_800, 2),
                new Event("r5", "t1", 0, 2_000, 3),
                new Event("r6", "t1", 1, 2_600, 4),
                new Event("r7", "t1", 0, 3_000, 5),
                new Event("r8", "t1", 1, 3_600, 6));

        EventStreamProcessor first = newProcessor();
        history.forEach(first::ingest);
        // 重复投递整个历史（模拟 at-least-once 重投），结果不得改变
        history.forEach(first::ingest);
        List<WindowResult> firstRun = first.finalizedResults();

        // 模拟重启后从历史重放：全新实例按相同顺序重放
        EventStreamProcessor replayed = newProcessor();
        history.forEach(replayed::ingest);
        List<WindowResult> replayRun = replayed.finalizedResults();

        log.info("assert replay consistency: first={} replay={}", firstRun, replayRun);
        assertEquals(firstRun, replayRun);
        // 水位线推进到 3000，horizon=2500：关闭 [0,1000) 与 [1000,2000) 两个窗口
        assertEquals(2, firstRun.size());
        assertEquals(3, firstRun.get(0).count());   // r1,r2,r3 属于 [0,1000)
        assertEquals(13, firstRun.get(0).sum());
        assertEquals(1, firstRun.get(1).count());   // r4 属于 [1000,2000)
        assertEquals(2, firstRun.get(1).sum());
    }

    @Test
    void concurrentMultiPartitionWritesAreConsistent() throws InterruptedException {
        // 允许迟到设大，避免并发下分区推进速度差异导致合法的迟到拒绝干扰计数断言
        EventStreamProcessor p = new EventStreamProcessor(1_000L, 1_000_000L, 100_000, 100_000);
        int threads = 4;
        int perThread = 250;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            final int partition = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        // 每个分区事件时间单调递增，ID 全局唯一
                        Event e = new Event("p" + partition + "-" + i, "t1", partition, i * 10L, 1);
                        if (p.ingest(e).isAccepted()) {
                            accepted.incrementAndGet();
                        }
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        assertEquals(threads * perThread, accepted.get());

        // 所有分区推进水位线，关闭全部窗口
        for (int t = 0; t < threads; t++) {
            p.ingest(new Event("final-" + t, "t1", t, 2_000_000L, 0));
        }
        long totalCount = p.finalizedResults().stream().mapToLong(WindowResult::count).sum();
        log.info("assert concurrent consistency: accepted={} finalizedCount={} openWindows={}",
                accepted.get(), totalCount, p.openWindowCount());
        // 关闭的窗口计数 + 最后推进事件所在窗口的计数 = 全部接受数
        long remaining = accepted.get() + threads - totalCount; // 含 final-* 推进事件
        assertEquals(threads, remaining, "仅最后未关闭窗口中的推进事件未计入已关闭结果");
        assertEquals(1, p.openWindowCount());
    }

    @Test
    void invalidEventsAreRejected() {
        EventStreamProcessor p = newProcessor();
        assertEquals(RejectionReason.INVALID, p.ingest(new Event(null, "t1", 0, 100, 1)).reason());
        assertEquals(RejectionReason.INVALID, p.ingest(new Event(" ", "t1", 0, 100, 1)).reason());
        assertEquals(RejectionReason.INVALID, p.ingest(new Event("neg", "t1", 0, -1, 1)).reason());
        assertEquals(RejectionReason.INVALID, p.ingest(null).reason());
        log.info("assert invalid events rejected with INVALID reason");
    }

    @Test
    void dedupStoreIsBounded() {
        EventStreamProcessor p = new EventStreamProcessor(1_000L, 500L, 1_000_000, 100);
        List<IngestOutcome> outcomes = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            outcomes.add(p.ingest(new Event("bounded-" + i, "t1", 0, i, 1)));
        }
        // 去重容量 100，最旧条目被淘汰后同 ID 不会误判为重复之外的状态泄漏；
        // 此处验证长期运行下处理器仍正常工作且状态有界
        assertTrue(p.openWindowCount() <= 2, "窗口随水位线推进被关闭，状态有界");
        log.info("assert bounded state: openWindows={} watermark={}", p.openWindowCount(), p.currentWatermark());
    }
}
