package com.github.lisama569.eventtimewindowstreamjava.web;

import com.github.lisama569.eventtimewindowstreamjava.stream.EventStreamProcessor;
import com.github.lisama569.eventtimewindowstreamjava.stream.TenantQuotas;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 从 {@link StreamProperties} 构造单例 {@link EventStreamProcessor}，
 * 供事件接入与 {@code /api/progress} 查询入口共用。
 */
@Configuration
@EnableConfigurationProperties(StreamProperties.class)
public class StreamConfig {

    @Bean
    EventStreamProcessor eventStreamProcessor(StreamProperties props) {
        Map<String, Integer> overrides = new LinkedHashMap<>();
        if (props.getTenantQuota() != null) {
            props.getTenantQuota().forEach((k, v) -> {
                if (k != null && !k.isBlank()) {
                    overrides.put(k, v);
                }
            });
        }
        return new EventStreamProcessor(props.getWindowSizeMillis(),
                props.getAllowedLatenessMillis(),
                new TenantQuotas(props.getDefaultTenantInflight(), overrides),
                props.getGlobalInflight(), props.getMaxDedupEntries(),
                props.getMaxTenants(), props.getMaxPartitionsPerTenant(),
                props.getIdleTenantEvictMillis(), System::currentTimeMillis);
    }
}
