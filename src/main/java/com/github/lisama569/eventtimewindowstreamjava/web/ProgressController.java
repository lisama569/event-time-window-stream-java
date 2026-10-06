package com.github.lisama569.eventtimewindowstreamjava.web;

import com.github.lisama569.eventtimewindowstreamjava.stream.EventStreamProcessor;
import com.github.lisama569.eventtimewindowstreamjava.stream.ProgressSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 处理进度查询入口，可从进程外直接读取。
 *
 * <ul>
 *   <li>{@code GET /api/progress}：全部租户快照；</li>
 *   <li>{@code GET /api/progress?tenant=tenantA}：只看指定租户。</li>
 * </ul>
 * 快照在处理器同一把锁内复制生成，多租户并发写入下各计数自洽。
 */
@RestController
@RequestMapping("/api/progress")
public class ProgressController {

    private static final Logger log = LoggerFactory.getLogger(ProgressController.class);

    private final EventStreamProcessor processor;

    public ProgressController(EventStreamProcessor processor) {
        this.processor = processor;
    }

    @GetMapping
    public ProgressSnapshot progress(@RequestParam(required = false) String tenant) {
        ProgressSnapshot snapshot = processor.progressSnapshot(tenant);
        log.info("progress queried tenantFilter={} activeTenants={} totalInFlight={} globalWatermark={}",
                tenant, snapshot.activeTenants(), snapshot.totalInFlight(), snapshot.globalWatermark());
        return snapshot;
    }
}
