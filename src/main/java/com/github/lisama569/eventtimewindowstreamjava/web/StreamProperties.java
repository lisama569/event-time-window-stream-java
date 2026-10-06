package com.github.lisama569.eventtimewindowstreamjava.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code stream.*} 应用属性。租户级额度通过 {@code stream.tenant-quota.<租户>=<额度>}
 * 绑定到 {@link #tenantQuota}（Spring relaxed binding）。
 */
@ConfigurationProperties(prefix = "stream")
public class StreamProperties {

    private long windowSizeMillis = 1_000L;
    private long allowedLatenessMillis = 500L;
    private int defaultTenantInflight = 10_000;
    private int globalInflight = 1_000_000;
    private int maxDedupEntries = 100_000;
    private int maxTenants = 128;
    private int maxPartitionsPerTenant = 1_024;
    private long idleTenantEvictMillis = 60_000L;
    /** 租户名 -> 该租户未定稿积压额度。 */
    private Map<String, Integer> tenantQuota = new LinkedHashMap<>();

    public long getWindowSizeMillis() {
        return windowSizeMillis;
    }

    public void setWindowSizeMillis(long windowSizeMillis) {
        this.windowSizeMillis = windowSizeMillis;
    }

    public long getAllowedLatenessMillis() {
        return allowedLatenessMillis;
    }

    public void setAllowedLatenessMillis(long allowedLatenessMillis) {
        this.allowedLatenessMillis = allowedLatenessMillis;
    }

    public int getDefaultTenantInflight() {
        return defaultTenantInflight;
    }

    public void setDefaultTenantInflight(int defaultTenantInflight) {
        this.defaultTenantInflight = defaultTenantInflight;
    }

    public int getGlobalInflight() {
        return globalInflight;
    }

    public void setGlobalInflight(int globalInflight) {
        this.globalInflight = globalInflight;
    }

    public int getMaxDedupEntries() {
        return maxDedupEntries;
    }

    public void setMaxDedupEntries(int maxDedupEntries) {
        this.maxDedupEntries = maxDedupEntries;
    }

    public int getMaxTenants() {
        return maxTenants;
    }

    public void setMaxTenants(int maxTenants) {
        this.maxTenants = maxTenants;
    }

    public int getMaxPartitionsPerTenant() {
        return maxPartitionsPerTenant;
    }

    public void setMaxPartitionsPerTenant(int maxPartitionsPerTenant) {
        this.maxPartitionsPerTenant = maxPartitionsPerTenant;
    }

    public long getIdleTenantEvictMillis() {
        return idleTenantEvictMillis;
    }

    public void setIdleTenantEvictMillis(long idleTenantEvictMillis) {
        this.idleTenantEvictMillis = idleTenantEvictMillis;
    }

    public Map<String, Integer> getTenantQuota() {
        return tenantQuota;
    }

    public void setTenantQuota(Map<String, Integer> tenantQuota) {
        this.tenantQuota = tenantQuota;
    }
}
