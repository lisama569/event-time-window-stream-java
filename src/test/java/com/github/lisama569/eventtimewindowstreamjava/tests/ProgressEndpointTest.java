package com.github.lisama569.eventtimewindowstreamjava.tests;

import com.github.lisama569.eventtimewindowstreamjava.stream.Event;
import com.github.lisama569.eventtimewindowstreamjava.stream.EventStreamProcessor;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 从进程外（HTTP）读取处理进度快照的端到端验证：
 * 容器启动真实 Bean（含属性绑定的租户额度），写入后经 REST 查询快照。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "stream.window-size-millis=1000",
                "stream.allowed-lateness-millis=500",
                "stream.default-tenant-inflight=10000",
                "stream.tenant-quota.flooder=2"
        })
class ProgressEndpointTest {

    private static final Logger log = LoggerFactory.getLogger(ProgressEndpointTest.class);

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private EventStreamProcessor processor;

    @Test
    @SuppressWarnings("unchecked")
    void progressSnapshotReadableOverHttpAndReflectsState() {
        processor.ingest(new Event("h1", "flooder", 0, 100, 1));
        processor.ingest(new Event("h2", "flooder", 0, 200, 1));
        processor.ingest(new Event("h3", "flooder", 0, 300, 1)); // 顶到 flooder 额度，被拒
        processor.ingest(new Event("g1", "gonormal", 1, 100, 9));

        String url = "http://localhost:" + port + "/api/progress";
        ResponseEntity<Map> resp = restTemplate.getForEntity(url, Map.class);
        assertTrue(resp.getStatusCode().is2xxSuccessful());
        Map<String, Object> body = resp.getBody();
        assertNotNull(body);
        var tenants = (java.util.List<Map<String, Object>>) body.get("tenants");
        assertEquals(2, tenants.size(), "HTTP 快照包含全部活跃租户");

        Map<String, Object> flooder = tenants.stream()
                .filter(t -> "flooder".equals(t.get("tenant"))).findFirst().orElseThrow();
        assertEquals(2, flooder.get("inFlight"));
        assertEquals(2, flooder.get("quota"), "HTTP 快照反映属性绑定的租户级额度");
        var rejects = (Map<String, Number>) flooder.get("rejectionCounts");
        assertEquals(1, rejects.get("TENANT_BACKLOG_FULL").intValue());

        var global = (Map<String, Number>) body.get("globalRejectionCounts");
        assertEquals(1, global.get("TENANT_BACKLOG_FULL").intValue());

        // 按租户过滤
        ResponseEntity<Map> filtered =
                restTemplate.getForEntity(url + "?tenant=gonormal", Map.class);
        var filteredTenants = (java.util.List<Map<String, Object>>) filtered.getBody().get("tenants");
        assertEquals(1, filteredTenants.size());
        assertEquals("gonormal", filteredTenants.get(0).get("tenant"));

        log.info("assert HTTP progress endpoint: tenants={} flooder={} globalRejects={}",
                tenants.size(), flooder, global);
    }
}
