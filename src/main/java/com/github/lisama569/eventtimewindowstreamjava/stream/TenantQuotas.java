package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.Map;

/**
 * 租户级额度配置：每个租户拥有自己的“未定稿事件积压”（在途事件）上限，
 * 某个租户顶到自己的上限只挡它自己，不影响其他租户。
 *
 * <p>配置本身有界：只为显式声明覆盖值的租户保存条目，未声明的租户使用默认额度。
 */
public final class TenantQuotas {

    private final int defaultMaxInFlight;
    private final Map<String, Integer> perTenantOverrides;

    public TenantQuotas(int defaultMaxInFlight, Map<String, Integer> perTenantOverrides) {
        if (defaultMaxInFlight <= 0) {
            throw new IllegalArgumentException("defaultMaxInFlight must be positive");
        }
        this.defaultMaxInFlight = defaultMaxInFlight;
        this.perTenantOverrides = perTenantOverrides == null ? Map.of() : Map.copyOf(perTenantOverrides);
        for (var entry : this.perTenantOverrides.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank() || entry.getValue() == null
                    || entry.getValue() <= 0) {
                throw new IllegalArgumentException("invalid tenant quota override: " + entry);
            }
        }
    }

    /** 返回指定租户的未定稿事件积压上限。 */
    public int maxInFlightFor(String tenant) {
        return perTenantOverrides.getOrDefault(tenant, defaultMaxInFlight);
    }

    public int defaultMaxInFlight() {
        return defaultMaxInFlight;
    }
}
