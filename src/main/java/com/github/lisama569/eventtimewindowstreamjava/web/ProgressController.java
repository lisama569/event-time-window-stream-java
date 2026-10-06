package com.github.lisama569.eventtimewindowstreamjava.web;

import com.github.lisama569.eventtimewindowstreamjava.stream.EventStreamProcessor;
import com.github.lisama569.eventtimewindowstreamjava.stream.ProgressSnapshot;
import com.github.lisama569.eventtimewindowstreamjava.stream.TenantProgress;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 从进程外读取处理进度快照的 HTTP 入口：{@code GET /api/stream/progress}。
 * 快照在处理器同一把锁内生成，并发写入下全局汇总与各租户、各分区明细自洽。
 * 可加 {@code ?tenant=xxx} 只看单个租户（全局汇总字段仍返回全量）。
 */
@RestController
@RequestMapping("/api/stream")
public class ProgressController {

    private final EventStreamProcessor processor;

    public ProgressController(EventStreamProcessor processor) {
        this.processor = processor;
    }

    @GetMapping("/progress")
    public ProgressSnapshot progress(@RequestParam(value = "tenant", required = false) String tenant) {
        ProgressSnapshot snapshot = processor.progressSnapshot();
        if (tenant == null || tenant.isBlank()) {
            return snapshot;
        }
        List<TenantProgress> filtered = snapshot.tenants().stream()
                .filter(t -> t.tenant().equals(tenant))
                .toList();
        return new ProgressSnapshot(
                snapshot.globalWatermarkMillis(),
                snapshot.openWindowCount(),
                snapshot.inFlight(),
                snapshot.rejectedByReason(),
                filtered);
    }
}
