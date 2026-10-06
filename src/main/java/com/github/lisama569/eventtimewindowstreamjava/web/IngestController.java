package com.github.lisama569.eventtimewindowstreamjava.web;

import com.github.lisama569.eventtimewindowstreamjava.stream.Event;
import com.github.lisama569.eventtimewindowstreamjava.stream.EventStreamProcessor;
import com.github.lisama569.eventtimewindowstreamjava.stream.IngestOutcome;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 事件接入 HTTP 入口，便于从进程外灌入数据并查看判定。
 *
 * <p>{@code POST /api/events}，JSON 体：eventId/tenant/partition/eventTimeMillis/amount。
 * 被拒绝时返回 429（容量/额度/迟到/重复），字段非法返回 400；
 * 响应体始终给出 {@code status} 与可区分的 {@code reason}。
 */
@RestController
@RequestMapping("/api/events")
public class IngestController {

    private final EventStreamProcessor processor;

    public IngestController(EventStreamProcessor processor) {
        this.processor = processor;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> ingest(@RequestBody EventRequest req) {
        Event event = new Event(req.getEventId(), req.getTenant(), req.getPartition(),
                req.getEventTimeMillis(), req.getAmount());
        IngestOutcome outcome = processor.ingest(event);
        Map<String, String> body = Map.of(
                "status", outcome.status().name(),
                "reason", outcome.reason() == null ? "" : outcome.reason().name());
        HttpStatus http = outcome.isAccepted()
                ? HttpStatus.OK
                : (outcome.reason() == com.github.lisama569.eventtimewindowstreamjava.stream.RejectionReason.INVALID
                        ? HttpStatus.BAD_REQUEST : HttpStatus.TOO_MANY_REQUESTS);
        return ResponseEntity.status(http).body(body);
    }
}
