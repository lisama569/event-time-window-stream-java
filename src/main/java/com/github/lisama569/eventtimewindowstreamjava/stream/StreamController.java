package com.github.lisama569.eventtimewindowstreamjava.stream;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 事件摄入与结果查询入口。
 */
@RestController
@RequestMapping("/stream")
public class StreamController {
    private final IngestGateway gateway;
    private final EventStreamProcessor processor;

    public StreamController(IngestGateway gateway, EventStreamProcessor processor) {
        this.gateway = gateway;
        this.processor = processor;
    }

    @PostMapping("/events")
    public ResponseEntity<IngestResult> ingest(@RequestBody Event event) {
        IngestResult result = gateway.submit(event);
        HttpStatus status = switch (result.status()) {
            case ACCEPTED, DUPLICATE -> HttpStatus.OK;
            case REJECTED_LATE -> HttpStatus.GONE;
            case REJECTED_BACKPRESSURE -> HttpStatus.TOO_MANY_REQUESTS;
        };
        return ResponseEntity.status(status).body(result);
    }

    @GetMapping("/windows/{tenantId}")
    public List<WindowResult> windows(@PathVariable String tenantId) {
        return processor.windows(tenantId);
    }

    @GetMapping("/watermarks/{tenantId}")
    public Map<String, Long> watermarks(@PathVariable String tenantId) {
        return processor.watermarks(tenantId);
    }
}
