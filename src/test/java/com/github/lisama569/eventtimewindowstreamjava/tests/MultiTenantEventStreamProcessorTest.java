package com.github.lisama569.eventtimewindowstreamjava.tests;

import com.github.lisama569.eventtimewindowstreamjava.stream.Event;
import com.github.lisama569.eventtimewindowstreamjava.stream.EventStreamProcessor;
import com.github.lisama569.eventtimewindowstreamjava.stream.IngestOutcome;
import com.github.lisama569.eventtimewindowstreamjava.stream.ProgressSnapshot;
import com.github.lisama569.eventtimewindowstreamjava.stream.RejectionReason;
import com.github.lisama569.eventtimewindowstreamjava.stream.TenantQuotas;
import com.github.lisama569.eventtimewindowstreamjava.stream.WindowResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多租户能力测试：租户级额度隔离、安静租户互不干扰、并发快照自洽、
 * 额度拒绝重试不被误判重复，以及租户状态的有界回收。
 */
class MultiTenantEventStreamProcessorTest {

    private static final Logger log = LoggerFactory.getLogger(MultiTenantEventStreamProcessorTest.class);

    /** 固定时钟，便于空闲回收测试确定性推进时间。 */
    private static EventStreamProcessor newProcessor(TenantQuotas quotas, int globalInFlight,
                                                     int maxTenants, long idleEvictMillis,
                                                     AtomicLong clock) {
        return new EventStreamProcessor(1_000L, 500L, quotas, globalInFlight,
                100_000, maxTenants, 128, idleEvictMillis, clock::get);
    }

    private static EventStreamProcessor newProcessor(TenantQuotas quotas) {
        return newProcessor(quotas, 1_000_000, 128, 60_000L, new AtomicLong(1_000L));
    }

    // ---------------------------------------------------------------------
    // 1. 某租户顶到自己额度，只挡它自己；其他租户照常推进、定稿不受影响
    // ---------------------------------------------------------------------
    @Test
    void oneTenantHittingQuotaDoesNotBlockOthers() {
        // flooder 额度 2，其他租户走默认额度 10000
        EventStreamProcessor p = newProcessor(new TenantQuotas(10_000, Map.of("flooder", 2)));

        assertTrue(p.ingest(new Event("f1", "flooder", 0, 100, 1)).isAccepted());
        assertTrue(p.ingest(new Event("f2", "flooder", 0, 200, 1)).isAccepted());
        IngestOutcome blocked = p.ingest(new Event("f3", "flooder", 0, 300, 1));
        assertEquals(RejectionReason.TENANT_BACKLOG_FULL, blocked.reason(),
                "顶到的是 flooder 自己的额度，拒绝原因须可与 LATE/DUPLICATE 区分");

        // 同一时刻，另一个租户照常写入并能推进定稿，完全不受 flooder 拖累
        assertTrue(p.ingest(new Event("a1", "normal", 0, 100, 5)).isAccepted());
        assertTrue(p.ingest(new Event("a2", "normal", 0, 200, 7)).isAccepted());
        assertTrue(p.ingest(new Event("adv", "normal", 0, 2_000, 0)).isAccepted());

        List<WindowResult> results = p.finalizedResults();
        assertEquals(1, results.size(), "只有 normal 自己定稿窗口");
        assertEquals("normal", results.get(0).key().tenant());
        assertEquals(12, results.get(0).sum());

        // flooder 的窗口没有被别人带着定稿，它仍积压 2 条；拒绝只记在它名下
        assertEquals(2, p.tenantInFlight("flooder"));
        ProgressSnapshot snap = p.progressSnapshot();
        ProgressSnapshot.TenantProgress flooder = findTenant(snap, "flooder");
        ProgressSnapshot.TenantProgress normal = findTenant(snap, "normal");
        assertEquals(1L, flooder.rejectionCounts().get("TENANT_BACKLOG_FULL"));
        assertEquals(0L, normal.rejectionCounts().get("TENANT_BACKLOG_FULL"));
        log.info("assert quota isolation: flooderRejected={} normalFinalizedSum={} flooderInFlight={}",
                flooder.rejectionCounts(), results.get(0).sum(), flooder.inFlight());
    }

    // ---------------------------------------------------------------------
    // 2. 安静/停写租户不卡住别人；别人推进也不把它的正常早到数据判成太晚
    // ---------------------------------------------------------------------
    @Test
    void quietTenantNeitherBlocksOthersNorGetsItsEarlyDataRejected() {
        EventStreamProcessor p = newProcessor(new TenantQuotas(10_000, Map.of()));

        // quiet 很早写一条后就停写；fast 一路狂飙到很远的事件时间
        assertTrue(p.ingest(new Event("q1", "quiet", 0, 100, 3)).isAccepted());
        for (long t = 1_000; t <= 20_000; t += 1_000) {
            assertTrue(p.ingest(new Event("fast-" + t, "fast", 0, t, 1)).isAccepted());
        }

        // fast 不停定稿，quiet 不卡它
        long fastFinalized = p.finalizedResults().stream()
                .filter(r -> r.key().tenant().equals("fast")).count();
        long quietFinalized = p.finalizedResults().stream()
                .filter(r -> r.key().tenant().equals("quiet")).count();
        assertTrue(fastFinalized >= 15, "fast 大量窗口已定稿");
        assertEquals(0, quietFinalized, "quiet 自己没推进，窗口不应被别人带着定稿");

        // quiet 终于回来：它的水位线还停在 100，早期窗口内的迟到数据仍合法，
        // 不会因为 fast 跑到 20000 而被误判 LATE
        IngestOutcome q2 = p.ingest(new Event("q2", "quiet", 0, 150, 4));
        assertTrue(q2.isAccepted(), "安静租户的正常早到数据不得被其他租户的水位线误伤");

        // quiet 自己推进后才定稿，且结果只含它自己的数据
        assertTrue(p.ingest(new Event("qadv", "quiet", 0, 2_000, 0)).isAccepted());
        WindowResult quietWindow = p.finalizedResults().stream()
                .filter(r -> r.key().tenant().equals("quiet"))
                .findFirst().orElse(null);
        assertNotNull(quietWindow);
        assertEquals(2, quietWindow.count());
        assertEquals(7, quietWindow.sum());
        log.info("assert quiet-tenant independence: fastFinalized={} quietWindowCount={} quietSum={}",
                fastFinalized, quietWindow.count(), quietWindow.sum());
    }

    // ---------------------------------------------------------------------
    // 3. 按租户/分区的进度快照在并发写入下自洽
    // ---------------------------------------------------------------------
    @Test
    void progressSnapshotIsSelfConsistentUnderConcurrentWrites() throws InterruptedException {
        // 允许迟到设大，避免并发推进速度差导致合法的迟到拒绝干扰断言
        EventStreamProcessor p = new EventStreamProcessor(
                1_000L, 10_000_000L, new TenantQuotas(1_000_000, Map.of()), 10_000_000,
                1_000_000, 64, 64, 0L, new AtomicLong(0L)::get);

        int tenants = 4;
        int partitions = 2;
        int perWorker = 300;
        ExecutorService pool = Executors.newFixedThreadPool(tenants * partitions);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        for (int tn = 0; tn < tenants; tn++) {
            for (int pt = 0; pt < partitions; pt++) {
                final String tenant = "t" + tn;
                final int partition = pt;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perWorker; i++) {
                            Event e = new Event(tenant + "-" + partition + "-" + i,
                                    tenant, partition, i * 10L, 1);
                            if (p.ingest(e).isAccepted()) {
                                accepted.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        }
        // 并发地不断读快照：任何时刻读到的数字都必须自洽，不得抛异常
        AtomicInteger queries = new AtomicInteger();
        ExecutorService readers = Executors.newFixedThreadPool(2);
        start.countDown();
        for (int r = 0; r < 2; r++) {
            readers.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    ProgressSnapshot s = p.progressSnapshot();
                    assertSnapshotInternallyConsistent(s);
                    queries.incrementAndGet();
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        readers.shutdownNow();
        readers.awaitTermination(5, TimeUnit.SECONDS);

        ProgressSnapshot snap = p.progressSnapshot();
        assertEquals(tenants, snap.tenants().size());
        // 总在途 = 各租户在途之和 = 接受总数（允许迟到很大、窗口基本都还开着）
        int sumTenantInFlight = snap.tenants().stream().mapToInt(ProgressSnapshot.TenantProgress::inFlight).sum();
        assertEquals(accepted.get(), snap.totalInFlight());
        assertEquals(accepted.get(), sumTenantInFlight);
        // 每租户每分区推进到的最大事件时间 = (perWorker-1)*10
        for (ProgressSnapshot.TenantProgress tp : snap.tenants()) {
            assertEquals(partitions, tp.partitions().size());
            for (ProgressSnapshot.PartitionProgress pp : tp.partitions()) {
                assertEquals((perWorker - 1) * 10L, pp.maxEventTime());
            }
        }
        log.info("assert concurrent snapshot consistency: accepted={} queries={} totalInFlight={}",
                accepted.get(), queries.get(), snap.totalInFlight());
    }

    private void assertSnapshotInternallyConsistent(ProgressSnapshot s) {
        int sumInFlight = 0;
        long sumTenantRejects = 0;
        for (ProgressSnapshot.TenantProgress tp : s.tenants()) {
            sumInFlight += tp.inFlight();
            long perTenantRejectTotal = tp.rejectionCounts().values().stream().mapToLong(Long::longValue).sum();
            // 单租户：各原因拒绝之和非负，且在途不超过额度
            assertTrue(tp.inFlight() <= tp.quota(), "租户在途不得超过其额度");
            sumTenantRejects += perTenantRejectTotal;
        }
        assertEquals(sumInFlight, s.totalInFlight(), "快照总在途必须等于各租户之和");
        long globalRejectTotal = s.globalRejectionCounts().values().stream().mapToLong(Long::longValue).sum();
        assertTrue(globalRejectTotal >= sumTenantRejects,
                "全局拒绝计数（含已回收租户）不应小于在驻租户拒绝之和");
    }

    // ---------------------------------------------------------------------
    // 4. 被额度拒绝的写入，降速后重投不会被误判为重复
    // ---------------------------------------------------------------------
    @Test
    void tenantQuotaRejectedWriteIsNotMarkedDuplicateAndCanBeRetried() {
        EventStreamProcessor p = newProcessor(new TenantQuotas(10_000, Map.of("a", 2)));

        assertTrue(p.ingest(new Event("a1", "a", 0, 100, 1)).isAccepted());
        assertTrue(p.ingest(new Event("a2", "a", 0, 200, 1)).isAccepted());

        Event e3 = new Event("a3", "a", 0, 300, 1);
        IngestOutcome first = p.ingest(e3);
        assertEquals(RejectionReason.TENANT_BACKLOG_FULL, first.reason());

        // 尚未释放容量时立刻重投：仍是额度问题，绝不是 DUPLICATE（被拒事件未进过去重表）
        IngestOutcome retryWhileFull = p.ingest(e3);
        assertEquals(RejectionReason.TENANT_BACKLOG_FULL, retryWhileFull.reason());
        assertEquals(0L, p.progressSnapshot().globalRejectionCounts().get("DUPLICATE"));

        // 租户降速：水位线推进关闭窗口、释放积压额度
        assertTrue(p.ingest(new Event("adv", "a", 0, 20_000, 0)).isAccepted());

        // 同一条事件重投，此时应被正常受理，而不是被判重复
        IngestOutcome retried = p.ingest(new Event("a3", "a", 0, 20_100, 1));
        assertTrue(retried.isAccepted(), "额度恢复后被拒事件重投必须成功，不得误判 DUPLICATE");

        // 只有真正受理过的事件再次投递才判 DUPLICATE
        assertEquals(RejectionReason.DUPLICATE,
                p.ingest(new Event("a3", "a", 0, 20_200, 1)).reason());
        log.info("assert rejected-write retry: quotaRejects=2 then ACCEPTED then DUPLICATE");
    }

    // ---------------------------------------------------------------------
    // 5. 租户注册表有界：长期安静（窗口已全部定稿）的租户状态被回收，计数结构不无限增长
    // ---------------------------------------------------------------------
    @Test
    void idleQuiescentTenantsAreEvictedSoStateStaysBounded() {
        AtomicLong clock = new AtomicLong(0L);
        // 最多驻留 2 个租户，距上次活动超过 1000 即可回收
        EventStreamProcessor p = newProcessor(
                new TenantQuotas(10_000, Map.of()), 1_000_000, 2, 1_000L, clock);

        assertTrue(p.ingest(new Event("x1", "old1", 0, 100, 1)).isAccepted());
        assertTrue(p.ingest(new Event("y1", "old2", 0, 100, 1)).isAccepted());
        clock.set(500L);
        assertTrue(p.ingest(new Event("xadv", "old1", 0, 1_500, 0)).isAccepted());
        assertTrue(p.ingest(new Event("yadv", "old2", 0, 1_500, 0)).isAccepted());
        assertEquals(2, p.progressSnapshot().activeTenants());

        // 足够久以后新租户到来：名额满，回收最久未活动的安静租户，驻留数仍有界
        clock.set(10_000L);
        assertTrue(p.ingest(new Event("z1", "new1", 0, 100, 1)).isAccepted());
        assertTrue(p.ingest(new Event("z2", "new2", 0, 100, 1)).isAccepted());

        ProgressSnapshot snap = p.progressSnapshot();
        assertEquals(2, snap.activeTenants(), "驻留租户数永不超过 maxTenants");
        // 已定稿的结果不受回收影响，仍可查到
        long finalizedOld = p.finalizedResults().stream()
                .filter(r -> r.key().tenant().startsWith("old")).count();
        assertEquals(2, finalizedOld, "被回收租户此前定稿的结果仍然保留");
        log.info("assert bounded tenants: activeTenants={} finalizedOldResults={}",
                snap.activeTenants(), finalizedOld);
    }

    private static ProgressSnapshot.TenantProgress findTenant(ProgressSnapshot snap, String tenant) {
        return snap.tenants().stream()
                .filter(t -> t.tenant().equals(tenant))
                .findFirst().orElseThrow();
    }
}
