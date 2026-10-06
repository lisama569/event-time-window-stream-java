package com.github.lisama569.eventtimewindowstreamjava.web;

import com.github.lisama569.eventtimewindowstreamjava.stream.EventStreamProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * 进程内事件流处理器的 Spring 装配。参数均可通过 application.properties/环境变量覆盖，
 * 缺省值适合本地验证：
 * <ul>
 *   <li>stream.window-size-millis / allowed-lateness-millis：窗口与允许迟到；</li>
 *   <li>stream.max-in-flight：全局在途上限（背压）；</li>
 *   <li>stream.tenant-backlog：每个租户的未定稿积压额度；</li>
 *   <li>stream.max-dedup-entries / max-tenants / max-partitions-per-tenant：有界状态上限。</li>
 * </ul>
 */
@Configuration
public class StreamConfig {

    @Value("${stream.window-size-millis:1000}")
    private long windowSizeMillis;

    @Value("${stream.allowed-lateness-millis:500}")
    private long allowedLatenessMillis;

    @Value("${stream.max-in-flight:10000}")
    private int maxInFlight;

    @Value("${stream.tenant-backlog:1000}")
    private int defaultTenantBacklog;

    @Value("${stream.max-dedup-entries:10000}")
    private int maxDedupEntries;

    @Value("${stream.max-tenants:1024}")
    private int maxTenants;

    @Value("${stream.max-partitions-per-tenant:1024}")
    private int maxPartitionsPerTenant;

    @Bean
    public EventStreamProcessor eventStreamProcessor() {
        return new EventStreamProcessor(
                windowSizeMillis,
                allowedLatenessMillis,
                maxInFlight,
                defaultTenantBacklog,
                maxDedupEntries,
                Map.of(),
                maxTenants,
                maxPartitionsPerTenant);
    }
}
