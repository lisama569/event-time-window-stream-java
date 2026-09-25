# event-time-window-stream-java

进程内本地实时事件流处理能力：以进程内状态模拟事件接入、事件时间窗口聚合与结果输出，
保证乱序到达、重复投递与并发写入下各入口结论一致且可解释。

## 核心概念

### 事件时间（Event Time）
每条 `Event` 携带 `eventTimeMillis`，窗口归属只由事件时间决定，与到达时刻无关：

```
windowStart = floor(eventTime / windowSize) * windowSize   // 窗口 [start, start+size)
```

窗口按租户（`tenant`）隔离：不同租户即使窗口边界相同，聚合状态也互不串扰。

### 水位线（Watermark）
- 每个分区独立记录已见最大事件时间；
- **全局水位线 = 各分区最大事件时间的最小值**，只增不减；
- 含义：水位线之前（考虑允许迟到后）的事件被认为已全部到达。

### 窗口关闭与结果确定
窗口在 `windowEnd + allowedLateness <= watermark` 时关闭并输出 `WindowResult`
（count、sum、输出时水位线）。关闭后结果不可再变——任何属于该窗口的事件
此后必然被判定为超出允许迟到范围而拒绝，因此同一窗口不会产生重复或矛盾的结果。

### 迟到（Lateness）
- `eventTime < watermark - allowedLateness`：以 `LATE_BEYOND_ALLOWED` **拒绝**，
  不静默丢弃；拒绝原因通过 `IngestOutcome.reason()` 可区分；
- 迟到但在允许范围内（`watermark - allowedLateness <= eventTime < watermark`）：
  正常接受并计入对应（尚未关闭的）窗口。

### 去重与幂等（Dedup / Idempotency）
- 相同 `eventId` 重复投递以 `DUPLICATE` 拒绝，不重复计数；
- 去重记录容量有界（`maxDedupEntries`），超出后按插入顺序淘汰最旧条目，
  长期运行无无界状态增长；
- 重启后从历史重放：处理结论只取决于事件内容与顺序，全新实例按相同顺序
  重放得到与首次完全一致的窗口结果（见测试 `replayFromHistoryYieldsIdenticalResults`）。

### 背压（Backpressure）
- 在途事件 = 已接受但所属窗口尚未关闭的事件；
- 在途数达到 `maxInFlight` 时，新事件以 `BACKPRESSURE_CAPACITY` 拒绝，
  生产端应降速后重试（被拒事件**不会**记录去重，重试不会误判为重复）；
- 豁免规则：若接受某事件所推进的水位线能关闭至少一个窗口（释放容量），
  即使在途已满也放行，避免“容量满 → 无法推进水位线 → 容量永不释放”的死锁。

### 并发与隔离
- 所有状态迁移在同一把锁内完成，支持多分区并发写入；
- 水位线按分区独立推进，窗口状态按租户隔离；
- 状态有界：去重记录（`maxDedupEntries`）、在途事件（`maxInFlight`）、
  结果缓冲（保留最近 1024 条窗口结果）均有上限。

## 拒绝原因一览

| 原因 | 含义 |
|---|---|
| `INVALID` | 字段非法（空 ID、负时间戳等） |
| `LATE_BEYOND_ALLOWED` | 事件时间早于 `watermark - allowedLateness` |
| `DUPLICATE` | 相同 `eventId` 已处理过 |
| `BACKPRESSURE_CAPACITY` | 在途事件数达到 `maxInFlight` |

## 使用示例

```java
EventStreamProcessor p = new EventStreamProcessor(
        1_000L,   // 窗口长度 1s
        500L,     // 允许迟到 500ms
        10_000,   // 在途上限（背压阈值）
        10_000);  // 去重容量

IngestOutcome outcome = p.ingest(new Event("id-1", "tenantA", 0, 1_700_000_000_000L, 42));
if (!outcome.isAccepted()) {
    // 按 outcome.reason() 区分处理：重试、降速或归入死信
}
List<WindowResult> results = p.finalizedResults();  // 已确定的窗口结果
```

## 验证方法

```bash
mvn test
```

测试覆盖（`EventStreamProcessorTest`）：

| 测试 | 验证点 |
|---|---|
| `outOfOrderEventsAggregateIntoSameWindowAndFinalizeOnWatermark` | 乱序事件归入正确窗口；水位线未越过边界前不输出结果 |
| `duplicateDeliveryIsRejectedAndCountedOnce` | 重复投递以 `DUPLICATE` 拒绝且只计一次 |
| `lateBeyondAllowedIsRejectedWithDistinguishableReason` | 超迟到拒绝原因可区分；允许范围内迟到仍接受 |
| `windowBoundaryEventBelongsToNextWindow` | 窗口边界事件归属 `[start, end)` 的下一个窗口 |
| `backpressureRejectsWhenInFlightFullAndRecoversAfterFinalize` | 背压拒绝、被拒事件可重试、水位线推进事件豁免 |
| `tenantStatesAreIsolated` | 租户状态隔离不串扰 |
| `replayFromHistoryYieldsIdenticalResults` | 重放/重启后结论与首次一致，历史重投不改变结果 |
| `concurrentMultiPartitionWritesAreConsistent` | 多分区并发写入计数一致、无状态泄漏 |
| `invalidEventsAreRejected` | 非法事件以 `INVALID` 拒绝 |
| `dedupStoreIsBounded` | 长期运行状态有界 |

单测日志（INFO 级）会打印每条事件的接受/拒绝判定及依据、水位线推进
（`watermark=old->new`）与窗口关闭结果（`window FINALIZED ... count=.. sum=..`），
可直接用于核对判定过程。
