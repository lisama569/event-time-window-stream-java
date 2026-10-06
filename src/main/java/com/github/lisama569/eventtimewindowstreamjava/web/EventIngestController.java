package com.github.lisama569.eventtimewindowstreamjava.web;

import com.github.lisama569.eventtimewindowstreamjava.stream.Event;
import com.github.lisama569.eventtimewindowstreamjava.stream.EventStreamProcessor;
import com.github.lisama569.eventtimewindowstreamjava.stream.IngestOutcome;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 从进程外写入事件的 HTTP 入口，便于本地端到端验证。
 * 同步返回该次接入的判定（接受或具体拒绝原因）。
 */
@RestController
@RequestMapping("/api/stream")
public class EventIngestController {

    private final EventStreamProcessor processor;

    public EventIngestController(EventStreamProcessor processor) {
        this.processor = processor;
    }

    @PostMapping("/events")
    public IngestOutcome ingest(@RequestBody Event event) {
        return processor.ingest(event);
    }
}
