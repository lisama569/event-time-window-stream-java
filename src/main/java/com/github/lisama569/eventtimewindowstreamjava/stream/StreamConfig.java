package com.github.lisama569.eventtimewindowstreamjava.stream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 流处理组件装配。
 */
@Configuration
public class StreamConfig {

    @Bean
    public EventStreamProcessor eventStreamProcessor(
            @Value("${stream.window-size-ms:60000}") long windowSizeMs,
            @Value("${stream.allowed-lateness-ms:30000}") long allowedLatenessMs,
            @Value("${stream.max-open-windows:1000}") int maxOpenWindows,
            @Value("${stream.dedup-capacity:100000}") int dedupCapacity) {
        return new EventStreamProcessor(windowSizeMs, allowedLatenessMs, maxOpenWindows, dedupCapacity);
    }

    @Bean
    public IngestGateway ingestGateway(EventStreamProcessor processor,
                                       @Value("${stream.max-concurrent-ingest:64}") int maxConcurrent) {
        return new IngestGateway(processor, maxConcurrent);
    }
}
