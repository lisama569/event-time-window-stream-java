package com.github.lisama569.eventtimewindowstreamjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 从进程外（HTTP）读取处理进度快照的端到端测试：真正启动 Web 容器，
 * 经 POST /api/stream/events 写入，再经 GET /api/stream/progress 核对
 * 按租户/分区的推进位置与按原因分类的拒绝数。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ProgressEndpointTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate rest;

    private String base() {
        return "http://localhost:" + port + "/api/stream";
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getProgress(String tenant) {
        String url = base() + "/progress" + (tenant == null ? "" : "?tenant=" + tenant);
        return rest.getForObject(url, Map.class);
    }

    private void postEvent(String id, String tenant, int partition, long eventTime, long amount) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = "{\"eventId\":\"%s\",\"tenant\":\"%s\",\"partition\":%d,\"eventTimeMillis\":%d,\"amount\":%d}"
                .formatted(id, tenant, partition, eventTime, amount);
        rest.postForEntity(base() + "/events", new HttpEntity<>(body, headers), Map.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void progressIsReadableOverHttpAndReflectsPerTenantState() {
        postEvent("http-a1", "httpA", 0, 100, 7);
        postEvent("http-a2", "httpA", 1, 300, 3);
        // 重复投递 -> DUPLICATE
        postEvent("http-a1", "httpA", 0, 100, 7);
        postEvent("http-b1", "httpB", 0, 100, 50);

        Map<String, Object> snap = getProgress(null);
        List<Map<String, Object>> tenants = (List<Map<String, Object>>) snap.get("tenants");
        assertTrue(tenants.stream().anyMatch(t -> t.get("tenant").equals("httpA")));
        assertTrue(tenants.stream().anyMatch(t -> t.get("tenant").equals("httpB")));

        Map<String, Object> a = tenants.stream().filter(t -> t.get("tenant").equals("httpA")).findFirst().orElseThrow();
        Map<String, Integer> rejected = (Map<String, Integer>) a.get("rejectedByReason");
        assertEquals(1, rejected.get("DUPLICATE"));

        List<Map<String, Object>> parts = (List<Map<String, Object>>) a.get("partitions");
        assertEquals(2, parts.size());
        // 租户水位线 = 各分区最大事件时间的最小值 = min(100, 300) = 100
        assertEquals(100, ((Number) a.get("watermarkMillis")).longValue());

        // 按租户过滤
        Map<String, Object> onlyB = getProgress("httpB");
        List<Map<String, Object>> bTenants = (List<Map<String, Object>>) onlyB.get("tenants");
        assertEquals(1, bTenants.size());
        assertEquals("httpB", bTenants.get(0).get("tenant"));
    }
}
