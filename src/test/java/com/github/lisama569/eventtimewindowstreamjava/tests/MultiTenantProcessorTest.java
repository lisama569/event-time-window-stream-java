package com.github.lisama569.eventtimewindowstreamjava.tests;

import com.github.lisama569.eventtimewindowstreamjava.stream.Event;
import com.github.lisama569.eventtimewindowstreamjava.stream.EventStreamProcessor;
import com.github.lisama569.eventtimewindowstreamjava.stream.IngestOutcome;
import com.github.lisama569.eventtimewindowstreamjava.stream.ProgressSnapshot;
import com.github.lisama569.eventtimewindowstreamjava.stream.RejectionReason;
import com.github.lisama569.eventtimewindowstreamjava.stream.TenantProgress;
import com.github.lisama569.eventtimewindowstreamjava.stream.WindowResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多租户能力测试：租户级积压额度隔离、安静租户边界、拒绝重试不误判、
 * 并发写入下进度快照自洽、租户/分区跟踪状态有界。
 */
class MultiTenantProcessorTest {

    private static final Logger log = LoggerFactory.getLogger(MultiTenantProcessorTest.class);

    /** 窗口 1s、允许迟到 500ms、租户额度 3、全局额度充裕。 */
    private static EventStreamProcessor processorWithTenantCap3() {
        return new EventStreamProcessor(1_000L, 500L, 100_000, 3, 100_000);
    }

    private static Event ev(String id, String tenant, int partition, long t, long amount) {
        return new Event(id, tenant, partition, t, amount);
    }

    @Test
    void oneTenantBacklogCapBlocksOnlyItselfAndOthersKeepProgressing() {
        EventStreamProcessor p = processorWithTenantCap3();
        // 租户 A 写满自身额度 3
        assertTrue(p.ingest(ev("a1", "A", 0, 100, 1)).isAccepted());
        assertTrue(p.ingest(ev("a2", "A", 0, 200, 1)).isAccepted());
        assertTrue(p.ingest(ev("a3", "A", 0, 300, 1)).isAccepted());

        // A 第 4 条被自己的租户额度挡下，原因明确区别于迟到/重复/全局背压
        IngestOutcome blocked = p.ingest(ev("a4", "A", 0, 400, 1));
        assertEquals(RejectionReason.TENANT_BACKLOG_LIMIT, blocked.reason());

        // 同一被拒事件立刻重投：仍是额度满，绝不能被判成 DUPLICATE（被拒即未受理）
        IngestOutcome retryStillBlocked = p.ingest(ev("a4", "A", 0, 400, 1));
        assertEquals(RejectionReason.TENANT_BACKLOG_LIMIT, retryStillBlocked.reason());

        // 租户 B 完全不受影响：能继续写、还能推进自己的水位线并定稿窗口
        assertTrue(p.ingest(ev("b1", "B", 0, 100, 10)).isAccepted());
        assertTrue(p.ingest(ev("b2", "B", 0, 200, 20)).isAccepted());
        assertTrue(p.ingest(ev("b3", "B", 0, 300, 30)).isAccepted());
        assertTrue(p.ingest(ev("b4", "B", 0, 1_600, 0)).isAccepted());
        long bSum = p.finalizedResults().stream()
                .filter(r -> r.key().tenant().equals("B")).mapToLong(WindowResult::sum).sum();
        log.info("assert tenant isolation: B finalized sum={} (expect 60) while A blocked", bSum);
        assertEquals(60, bSum, "B 的窗口应能独立定稿，不被 A 拖累");

        // 快照里 A 的拒绝原因能单独看出是 TENANT_BACKLOG_LIMIT
        ProgressSnapshot snap = p.progressSnapshot();
        TenantProgress a = snap.tenants().stream().filter(t -> t.tenant().equals("A")).findFirst().orElseThrow();
        assertEquals(2, a.rejectedByReason().get(RejectionReason.TENANT_BACKLOG_LIMIT));
        assertEquals(0, a.rejectedByReason().get(RejectionReason.DUPLICATE));
        TenantProgress b = snap.tenants().stream().filter(t -> t.tenant().equals("B")).findFirst().orElseThrow();
        assertEquals(0, b.rejectedByReason().values().stream().mapToInt(Integer::intValue).sum(),
                "被挡的只是 A，B 不应有任何拒绝");

        // A 降速后重投此前被拒的同 ID：窗口定稿释放额度后成功受理，而不是 DUPLICATE
        assertTrue(p.ingest(ev("advA", "A", 0, 5_000, 0)).isAccepted());
        IngestOutcome retryAccepted = p.ingest(ev("a4", "A", 0, 5_100, 5));
        assertEquals(IngestOutcome.Status.ACCEPTED, retryAccepted.status(),
                "被额度拒绝的事件重投不得误判为重复");
        log.info("assert rejected-then-retry accepted (not duplicate): {}", retryAccepted.status());
    }

    @Test
    void quietTenantDoesNotBlockOrRejectOtherTenants() {
        EventStreamProcessor p = processorWithTenantCap3();
        // A 用两个分区，B 安静（只有一条很早的数据），C 也有一条很早的数据
        assertTrue(p.ingest(ev("a1", "A", 0, 100, 1)).isAccepted());
        assertTrue(p.ingest(ev("a2", "A", 1, 200, 1)).isAccepted());
        assertTrue(p.ingest(ev("b1", "B", 0, 100, 100)).isAccepted());
        assertTrue(p.ingest(ev("c1", "C", 0, 100, 10)).isAccepted());

        // 只推进 A 自己的两个分区，A 的窗口就应能定稿，无需等 B/C
        assertTrue(p.ingest(ev("aAdv0", "A", 0, 1_600, 0)).isAccepted());
        assertTrue(p.ingest(ev("aAdv1", "A", 1, 1_700, 0)).isAccepted());
        long aSum = p.finalizedResults().stream()
                .filter(r -> r.key().tenant().equals("A")).mapToLong(WindowResult::sum).sum();
        assertEquals(2, aSum, "A 的窗口只靠 A 自己的数据即可定稿");

        // B 猛地跑到很前面：不得把 C 的正常数据判成太晚，也不得替 C 关窗
        assertTrue(p.ingest(ev("bAdv", "B", 0, 9_000, 0)).isAccepted());
        IngestOutcome cNormal = p.ingest(ev("c2", "C", 0, 200, 20));
        assertEquals(IngestOutcome.Status.ACCEPTED, cNormal.status(), "其他租户跑得快不应让 C 的早到数据变晚");

        // C 自己推进后，它的窗口定稿；B 的窗口此时也已定稿
        assertTrue(p.ingest(ev("cAdv", "C", 0, 5_000, 0)).isAccepted());
        WindowResult cWindow = p.finalizedResults().stream()
                .filter(r -> r.key().tenant().equals("C")).findFirst().orElseThrow();
        assertEquals(2, cWindow.count());
        assertEquals(30, cWindow.sum());
        // B 的定稿时间完全由 B 自己的数据决定，sum=100
        long bSum = p.finalizedResults().stream()
                .filter(r -> r.key().tenant().equals("B")).mapToLong(WindowResult::sum).sum();
        assertEquals(100, bSum);
        log.info("assert quiet/fast tenants isolated: A={} B={} C={}", aSum, bSum, cWindow.sum());
    }

    @Test
    void concurrentMultiTenantWritesProduceSelfConsistentSnapshots() throws InterruptedException {
        // 允许迟到设大，避免正常调度抖动产生合法的迟到拒绝，专注验证计数自洽
        EventStreamProcessor p = new EventStreamProcessor(1_000L, 1_000_000L, 100_000, 2_000, 100_000);
        int tenants = 8;
        int partitionsPerTenant = 2;
        int perTenant = 1_000;
        AtomicLong accepted = new AtomicLong();
        AtomicLong rejected = new AtomicLong();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(tenants);
        for (int t = 0; t < tenants; t++) {
            final String tenant = "T" + t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perTenant; i++) {
                        int partition = i % partitionsPerTenant;
                        Event e = ev(tenant + "-" + i, tenant, partition, i * 10L, 1);
                        if (p.ingest(e).isAccepted()) {
                            accepted.incrementAndGet();
                        } else {
                            rejected.incrementAndGet();
                        }
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        // 并发读快照：每次读到的全局与各租户计数都必须自洽
        ExecutorService readers = Executors.newFixedThreadPool(2);
        CountDownLatch stopReaders = new CountDownLatch(1);
        for (int r = 0; r < 2; r++) {
            readers.submit(() -> {
                while (stopReaders.getCount() > 0) {
                    assertSnapshotSelfConsistent(p.progressSnapshot());
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        stopReaders.countDown();
        readers.shutdown();
        assertTrue(readers.awaitTermination(10, TimeUnit.SECONDS));

        ProgressSnapshot snap = p.progressSnapshot();
        assertSnapshotSelfConsistent(snap);
        long attempts = (long) tenants * perTenant;
        assertEquals(attempts, accepted.get() + rejected.get());
        long totalRejectedByCounters = snap.rejectedByReason().values().stream()
                .mapToLong(Integer::longValue).sum();
        assertEquals(rejected.get(), totalRejectedByCounters,
                "调用方观察到的拒绝数应等于快照里按原因汇总的拒绝数");
        log.info("assert concurrent snapshot consistent: attempts={} accepted={} rejected={} openWindows={} inFlight={}",
                attempts, accepted.get(), rejected.get(), snap.openWindowCount(), snap.inFlight());
        assertEquals(tenants, snap.tenants().size());
    }

    private void assertSnapshotSelfConsistent(ProgressSnapshot snap) {
        int sumTenantInFlight = snap.tenants().stream().mapToInt(TenantProgress::inFlight).sum();
        int sumTenantWindows = snap.tenants().stream().mapToInt(TenantProgress::openWindowCount).sum();
        assertEquals(snap.inFlight(), sumTenantInFlight, "全局在途数必须等于各租户在途数之和");
        assertEquals(snap.openWindowCount(), sumTenantWindows, "全局未定稿窗口数必须等于各租户之和");
        for (var reason : RejectionReason.values()) {
            int sumTenantReason = snap.tenants().stream()
                    .mapToInt(t -> t.rejectedByReason().get(reason)).sum();
            // INVALID 且 tenant 为空的拒绝不计入任何租户，故租户之和 <= 全局
            assertTrue(sumTenantReason <= snap.rejectedByReason().get(reason),
                    "各租户 " + reason + " 拒绝数之和不得超过全局计数");
        }
        for (TenantProgress t : snap.tenants()) {
            assertNotNull(t.tenant());
            // 每个分区的最大事件时间不得超过该租户水位线以外的定义：分区最小值即水位线
            if (!t.partitions().isEmpty() && t.watermarkMillis() != null) {
                long minPartition = t.partitions().stream()
                        .mapToLong(pp -> pp.maxEventTimeMillis()).min().orElseThrow();
                assertEquals(t.watermarkMillis().longValue(), minPartition,
                        "租户水位线应等于其各分区最大事件时间的最小值");
            }
        }
    }

    @Test
    void rejectedByLateOrDuplicateAreCountedDistinctly() {
        EventStreamProcessor p = processorWithTenantCap3();
        assertTrue(p.ingest(ev("a1", "A", 0, 5_000, 1)).isAccepted());
        assertEquals(RejectionReason.LATE_BEYOND_ALLOWED,
                p.ingest(ev("late", "A", 0, 4_000, 1)).reason());
        assertEquals(RejectionReason.DUPLICATE,
                p.ingest(ev("a1", "A", 0, 5_000, 1)).reason());
        ProgressSnapshot snap = p.progressSnapshot();
        TenantProgress a = snap.tenants().stream().filter(t -> t.tenant().equals("A")).findFirst().orElseThrow();
        assertEquals(1, a.rejectedByReason().get(RejectionReason.LATE_BEYOND_ALLOWED));
        assertEquals(1, a.rejectedByReason().get(RejectionReason.DUPLICATE));
        assertEquals(0, a.rejectedByReason().get(RejectionReason.TENANT_BACKLOG_LIMIT));
        log.info("assert rejection reasons separated per tenant: {}", a.rejectedByReason());
    }

    @Test
    void tenantAndPartitionTrackersAreBounded() {
        // 租户数上限 4，每租户分区上限 2
        EventStreamProcessor p = new EventStreamProcessor(
                1_000L, 500L, 1_000_000, 1_000_000, 1_000_000,
                java.util.Map.of(), 4, 2);
        // 8 个租户各发一个足够远的事件，窗口立刻定稿、在途归零，空闲租户可被淘汰
        for (int i = 0; i < 8; i++) {
            assertTrue(p.ingest(ev("t-" + i, "tenant" + i, 0, 5_000_000L + i, 1)).isAccepted());
        }
        assertTrue(p.progressSnapshot().tenants().size() <= 4, "空闲租户被有界淘汰");

        // 单个租户使用超过 2 个分区，跟踪器按最旧分区淘汰，分区视图不超过上限
        EventStreamProcessor q = new EventStreamProcessor(
                1_000L, 500L, 1_000_000, 1_000_000, 1_000_000,
                java.util.Map.of(), 4, 2);
        assertTrue(q.ingest(ev("q0", "Q", 0, 100, 1)).isAccepted());
        assertTrue(q.ingest(ev("q1", "Q", 1, 200, 1)).isAccepted());
        assertTrue(q.ingest(ev("q2", "Q", 2, 300, 1)).isAccepted());
        TenantProgress qt = q.progressSnapshot().tenants().get(0);
        assertEquals(2, qt.partitions().size(), "每租户分区跟踪数有界");

        // 淘汰一个仍有未定稿窗口的租户后，全局在途/窗口计数必须扣除干净，快照仍自洽
        int tenantInFlight = q.progressSnapshot().tenants().stream()
                .mapToInt(TenantProgress::inFlight).sum();
        assertTrue(tenantInFlight >= 3, "前置：Q 当前有 3 条在途事件");
        // 再压入 4 个新租户触发对 Q 的 LRU 淘汰（上限 4：第 4 个新租户写入时淘汰最旧的 Q）
        for (int i = 3; i <= 6; i++) {
            assertTrue(q.ingest(ev("n" + i, "N" + i, 0, 100 + i, 1)).isAccepted());
        }
        ProgressSnapshot afterEvict = q.progressSnapshot();
        assertSnapshotSelfConsistent(afterEvict);
        assertTrue(afterEvict.tenants().stream().noneMatch(t -> t.tenant().equals("Q")),
                "最久未写入的 Q 应被淘汰");
        log.info("assert bounded trackers: tenants={} partitions={} inFlightAfterEvict={}",
                p.progressSnapshot().tenants().size(), qt.partitions().size(), afterEvict.inFlight());
    }

    @Test
    void emptySnapshotIsExposedWhenNoData() {
        EventStreamProcessor p = processorWithTenantCap3();
        ProgressSnapshot snap = p.progressSnapshot();
        assertNull(snap.globalWatermarkMillis());
        assertEquals(0, snap.inFlight());
        assertEquals(0, snap.openWindowCount());
        assertTrue(snap.tenants().isEmpty());
    }
}
