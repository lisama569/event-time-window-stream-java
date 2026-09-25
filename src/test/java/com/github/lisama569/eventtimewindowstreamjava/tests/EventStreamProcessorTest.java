package com.github.lisama569.eventtimewindowstreamjava.tests;

import com.github.lisama569.eventtimewindowstreamjava.stream.Event;
import com.github.lisama569.eventtimewindowstreamjava.stream.EventStreamProcessor;
import com.github.lisama569.eventtimewindowstreamjava.stream.IngestGateway;
import com.github.lisama569.eventtimewindowstreamjava.stream.IngestResult;
import com.github.lisama569.eventtimewindowstreamjava.stream.IngestStatus;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖乱序、重复投递、窗口边界、重放一致性、并发隔离与背压。
 * 日志打印关键事件、水位线与判定依据。
 */
class EventStreamProcessorTest {
    private static final Logger log = LoggerFactory.getLogger(EventStreamProcessorTest.class);

    private static final long WINDOW = 60_000;      // 1 分钟窗口
    private static final long LATENESS = 30_000;    // 允许迟到 30 秒

    private EventStreamProcessor newProcessor() {
        return new EventStreamProcessor(WINDOW, LATENESS, 100, 10_000);
    }

    private static Event ev(String id, String tenant, int partition, long eventTime, long value) {
        return new Event(id, tenant, partition, eventTime, value);
    }

    private void logResult(String tag, Event e, IngestResult r, EventStreamProcessor p) {
        log.info("[{}] event={} -> status={}, reason={}, watermarks={}",
                tag, e.eventId(), r.status(), r.reason(), p.watermarks(e.tenantId()));
    }

    @Test
    void outOfOrderWithinLatenessIsAccepted() {
        EventStreamProcessor p = newProcessor();
        // 先推进水位线：maxEventTime=120_000 -> watermark=90_000
        logResult("乱序-基准", ev("e1", "t1", 0, 120_000, 1), p.ingest(ev("e1", "t1", 0, 120_000, 1)), p);
        // 乱序事件 100_000 > watermark 90_000，应被接受，落入窗口 [60s,120s)
        IngestResult r = p.ingest(ev("e2", "t1", 0, 100_000, 5));
        logResult("乱序-容忍内", ev("e2", "t1", 0, 100_000, 5), r, p);
        assertEquals(IngestStatus.ACCEPTED, r.status());

        List<WindowResult> windows = p.windows("t1");
        windows.forEach(w -> log.info("[乱序-窗口] {}", w));
        WindowResult w = windows.stream().filter(x -> x.key().windowStart() == 60_000).findFirst().orElseThrow();
        assertEquals(1, w.count());
        assertEquals(5, w.sum());
    }

    @Test
    void lateBeyondWatermarkIsRejectedWithDistinctReason() {
        EventStreamProcessor p = newProcessor();
        logResult("迟到-基准", ev("e1", "t1", 0, 120_000, 1), p.ingest(ev("e1", "t1", 0, 120_000, 1)), p);
        // watermark=90_000；事件 80_000 <= watermark，超出允许迟到范围
        IngestResult r = p.ingest(ev("e2", "t1", 0, 80_000, 5));
        logResult("迟到-拒绝", ev("e2", "t1", 0, 80_000, 5), r, p);
        assertEquals(IngestStatus.REJECTED_LATE, r.status());
        assertTrue(r.reason().contains("水位线"), "拒绝原因需可解释");
        // 被拒绝的事件不得计入任何窗口
        assertEquals(1, p.windows("t1").stream().mapToLong(WindowResult::count).sum());
    }

    @Test
    void duplicateDeliveryIsIdempotent() {
        EventStreamProcessor p = newProcessor();
        Event e = ev("dup-1", "t1", 0, 100_000, 7);
        logResult("去重-首次", e, p.ingest(e), p);
        IngestResult dup = p.ingest(e);
        logResult("去重-重复", e, dup, p);
        assertEquals(IngestStatus.DUPLICATE, dup.status());
        WindowResult w = p.windows("t1").get(0);
        log.info("[去重-窗口] {}", w);
        assertEquals(1, w.count(), "重复投递不得重复计入");
        assertEquals(7, w.sum());
    }

    @Test
    void windowFinalizesOnlyAfterWatermarkPassesBoundary() {
        EventStreamProcessor p = newProcessor();
        // 窗口边界事件：恰好落在窗口起点 60_000（左闭）
        logResult("边界-左闭", ev("b1", "t1", 0, 60_000, 2), p.ingest(ev("b1", "t1", 0, 60_000, 2)), p);
        // 恰好落在窗口右边界 120_000（右开，应进入下一窗口）
        logResult("边界-右开", ev("b2", "t1", 0, 120_000, 3), p.ingest(ev("b2", "t1", 0, 120_000, 3)), p);

        WindowResult w1 = p.windows("t1").stream()
                .filter(w -> w.key().windowStart() == 60_000).findFirst().orElseThrow();
        log.info("[边界-未确定] {}", w1);
        assertFalse(w1.finalized(), "水位线 90_000 未越过窗口右边界 120_000，窗口不得确定");

        // 推进事件时间到 150_001 -> watermark=120_001 > 120_000，窗口 [60s,120s) 确定
        logResult("边界-推进", ev("b3", "t1", 0, 150_001, 1),
                p.ingest(ev("b3", "t1", 0, 150_001, 1)), p);
        WindowResult w1f = p.windows("t1").stream()
                .filter(w -> w.key().windowStart() == 60_000).findFirst().orElseThrow();
        log.info("[边界-已确定] {}", w1f);
        assertTrue(w1f.finalized(), "水位线越过窗口右边界后窗口必须确定");
        assertEquals(1, w1f.count());
        assertEquals(2, w1f.sum());
    }

    @Test
    void replayAfterResetYieldsIdenticalResults() {
        EventStreamProcessor p = newProcessor();
        List<Event> history = List.of(
                ev("r1", "t1", 0, 60_000, 1),
                ev("r2", "t1", 1, 61_000, 2),
                ev("r3", "t1", 0, 130_000, 3),
                ev("r2", "t1", 1, 61_000, 2), // 重复投递
                ev("r4", "t1", 1, 200_000, 4));
        for (Event e : history) {
            logResult("重放-首次", e, p.ingest(e), p);
        }
        List<WindowResult> first = p.windows("t1");
        first.forEach(w -> log.info("[重放-首次窗口] {}", w));

        p.reset();
        log.info("[重放] 已重置状态，从历史重放 {} 条事件", history.size());
        for (Event e : history) {
            logResult("重放-再次", e, p.ingest(e), p);
        }
        List<WindowResult> replayed = p.windows("t1");
        replayed.forEach(w -> log.info("[重放-再次窗口] {}", w));
        assertEquals(first, replayed, "重放后聚合结论必须与首次一致");
    }

    @Test
    void concurrentMultiPartitionWritesAreIsolatedAndConsistent() throws InterruptedException {
        EventStreamProcessor p = newProcessor();
        int partitions = 4;
        int perPartition = 500;
        ExecutorService pool = Executors.newFixedThreadPool(partitions);
        CountDownLatch done = new CountDownLatch(partitions);
        AtomicInteger accepted = new AtomicInteger();
        for (int part = 0; part < partitions; part++) {
            final int pp = part;
            pool.submit(() -> {
                try {
                    for (int i = 0; i < perPartition; i++) {
                        // 每个分区独立事件 id，时间单调推进，落在同一窗口 [0,60s)
                        Event e = ev("p" + pp + "-e" + i, "t1", pp, 1_000 + i, 1);
                        if (p.ingest(e).status() == IngestStatus.ACCEPTED) {
                            accepted.incrementAndGet();
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();
        log.info("[并发] accepted={}, watermarks={}", accepted.get(), p.watermarks("t1"));

        List<WindowResult> windows = p.windows("t1");
        windows.forEach(w -> log.info("[并发-窗口] {}", w));
        long total = windows.stream().mapToLong(WindowResult::count).sum();
        assertEquals(partitions * perPartition, total, "并发写入不得丢失或串扰");
        assertEquals(accepted.get(), total);

        // 租户隔离：另一租户无任何状态
        assertTrue(p.windows("other-tenant").isEmpty(), "不同租户状态必须隔离");
    }

    @Test
    void backpressureRejectsWhenCapacityExhausted() throws InterruptedException {
        EventStreamProcessor p = newProcessor();
        // 许可为 1 的网关：用一个线程长期占用许可，期间另一提交必须被背压拒绝
        IngestGateway gateway = new IngestGateway(p, 1);
        CountDownLatch permitHeld = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // 通过反射以外的可控方式占住许可：构造一个提交期间阻塞的处理器
        EventStreamProcessor slow = new EventStreamProcessor(WINDOW, LATENESS, 100, 10_000) {
            @Override
            public IngestResult ingest(Event event) {
                permitHeld.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
                return super.ingest(event);
            }
        };
        IngestGateway slowGateway = new IngestGateway(slow, 1);
        Thread holder = new Thread(() -> slowGateway.submit(ev("hold", "t1", 0, 100_000, 1)));
        holder.start();
        assertTrue(permitHeld.await(5, TimeUnit.SECONDS), "许可应已被占用");

        IngestResult r = slowGateway.submit(ev("g2", "t1", 0, 101_000, 1));
        log.info("[背压] status={}, reason={}", r.status(), r.reason());
        assertEquals(IngestStatus.REJECTED_BACKPRESSURE, r.status());
        assertTrue(r.reason().contains("许可耗尽"), "背压拒绝原因需可区分");

        release.countDown();
        holder.join(5_000);
        // 许可释放后恢复正常摄入
        IngestResult after = slowGateway.submit(ev("g3", "t1", 0, 102_000, 1));
        log.info("[背压-恢复] status={}, reason={}", after.status(), after.reason());
        assertEquals(IngestStatus.ACCEPTED, after.status());
    }

    @Test
    void stateIsBoundedByMaxOpenWindows() {
        // maxOpenWindows=2：窗口确定后被淘汰，状态有界
        EventStreamProcessor p = new EventStreamProcessor(WINDOW, 0, 2, 10_000);
        List<Event> events = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            events.add(ev("w" + i, "t1", 0, i * WINDOW + 1, 1));
        }
        for (Event e : events) {
            logResult("有界", e, p.ingest(e), p);
        }
        List<WindowResult> windows = p.windows("t1");
        windows.forEach(w -> log.info("[有界-窗口] {}", w));
        assertTrue(windows.size() <= 2, "已确定窗口超出上限后必须被淘汰，状态不得无界增长");
    }
}
