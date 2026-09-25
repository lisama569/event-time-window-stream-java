# 本地实时事件流处理（事件时间窗口聚合）

进程内模拟事件接入、事件时间窗口聚合与结果输出，保证乱序到达、重复投递与并发写入下各入口结论一致且可解释。

## 核心概念与规则

### 事件时间（Event Time）
每条 `Event(eventId, tenantId, partition, eventTime, value)` 携带事件时间（epoch 毫秒）。
事件按事件时间落入左闭右开的滚动窗口 `[windowStart, windowStart + windowSize)`：
- 恰好等于窗口起点的事件属于该窗口（左闭）；
- 恰好等于窗口右边界的事件属于下一个窗口（右开）。

### 水位线（Watermark）
每个 `(tenantId, partition)` 独立维护水位线：

```
watermark = max(已见事件时间) - allowedLatenessMs
```

水位线只增不减。**只有水位线推进到窗口右边界之后（watermark >= windowEnd），窗口结果才判定为确定（finalized=true）**。
跨分区查询时，窗口需该租户所有分区的水位线都越过右边界才判定确定。

### 迟到事件
事件时间 `<=` 当前水位线的事件视为超出允许迟到范围：
- **不静默丢弃**，返回 `REJECTED_LATE`，reason 中携带事件时间、当前水位线与允许迟到毫秒数，可区分、可解释；
- 被拒绝的事件不计入任何窗口。

### 去重与幂等
- 每个分区维护有界 LRU 去重索引（`dedupCapacity`），按 `eventId` 去重；
- 重复投递返回 `DUPLICATE`，不重复计入窗口，聚合结果幂等；
- `reset()` 清空全部状态后从历史重放相同事件序列，聚合结论与首次完全一致（测试 `replayAfterResetYieldsIdenticalResults` 验证）。

### 并发隔离与背压
- 状态按 `(tenantId, partition)` 隔离，分区内串行（synchronized）、分区间并行，互不串扰；
- 生产端入口 `IngestGateway` 用有界并发许可实现背压：许可耗尽时**立即**返回 `REJECTED_BACKPRESSURE`，不阻塞、不无界排队；
- 每个分区最多保留 `maxOpenWindows` 个窗口，已确定的最旧窗口被自动淘汰，长期运行状态有界。

### 摄入结果状态（可区分原因）

| 状态 | 含义 | HTTP 语义 |
|---|---|---|
| `ACCEPTED` | 正常计入窗口 | 200 |
| `DUPLICATE` | 重复投递，幂等忽略 | 200 |
| `REJECTED_LATE` | 超出允许迟到范围，拒绝 | 410 |
| `REJECTED_BACKPRESSURE` | 处理能力不足，背压拒绝 | 429 |

## 配置项（application.properties）

| 键 | 默认值 | 说明 |
|---|---|---|
| `stream.window-size-ms` | 60000 | 窗口大小 |
| `stream.allowed-lateness-ms` | 30000 | 允许迟到（决定水位线） |
| `stream.max-open-windows` | 1000 | 每分区最大窗口数 |
| `stream.dedup-capacity` | 100000 | 每分区去重索引容量 |
| `stream.max-concurrent-ingest` | 64 | 摄入并发许可数 |

## REST 入口

- `POST /stream/events`：摄入事件，返回 `IngestResult{status, reason}`
- `GET /stream/windows/{tenantId}`：查询租户窗口聚合结果（含 finalized 标记）
- `GET /stream/watermarks/{tenantId}`：查询租户各分区水位线

## 验证方法

运行全部测试：

```bash
mvn test
```

`EventStreamProcessorTest` 覆盖：

| 测试 | 场景 |
|---|---|
| `outOfOrderWithinLatenessIsAccepted` | 允许迟到范围内的乱序事件被正确计入 |
| `lateBeyondWatermarkIsRejectedWithDistinctReason` | 超出迟到范围按可区分原因拒绝且不计入 |
| `duplicateDeliveryIsIdempotent` | 重复投递幂等，不重复计数 |
| `windowFinalizesOnlyAfterWatermarkPassesBoundary` | 窗口边界左闭右开、水位线越界后才确定 |
| `replayAfterResetYieldsIdenticalResults` | 重置重放后结论与首次一致 |
| `concurrentMultiPartitionWritesAreIsolatedAndConsistent` | 4 分区并发写入无丢失、无串扰、租户隔离 |
| `backpressureRejectsWhenCapacityExhausted` | 许可耗尽时背压拒绝，释放后恢复 |
| `stateIsBoundedByMaxOpenWindows` | 已确定窗口超限被淘汰，状态有界 |

测试日志打印关键事件、水位线与判定依据，可在
`target/surefire-reports/TEST-...EventStreamProcessorTest.xml` 的 `system-out` 中查看，例如：

```
[迟到-拒绝] event=e2 -> status=REJECTED_LATE, reason=eventTime=80000 不晚于当前水位线=90000（允许迟到=30000ms），窗口已确定，拒绝计入, watermarks={t1#0=90000}
[边界-已确定] WindowResult[key=WindowKey[tenantId=t1, windowStart=60000, windowEnd=120000], count=1, sum=2, finalized=true]
```
