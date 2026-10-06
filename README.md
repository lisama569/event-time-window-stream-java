# event-time-window-stream-java

进程内本地实时事件流处理能力：以进程内状态模拟事件接入、事件时间窗口聚合与结果输出，
保证乱序到达、重复投递、并发写入与**多租户共用**下各入口结论一致且可解释。

## 核心概念

### 事件时间（Event Time）
每条 `Event` 携带 `eventTimeMillis`，窗口归属只由事件时间决定，与到达时刻无关：

```
windowStart = floor(eventTime / windowSize) * windowSize   // 窗口 [start, start+size)
```

窗口按租户（`tenant`）隔离：不同租户即使窗口边界相同，聚合状态也互不串扰。

### 水位线（Watermark，按租户独立）
- 每个 **(租户, 分区)** 独立记录已见最大事件时间；
- **租户水位线 = 该租户各分区最大事件时间的最小值**，只增不减；
- 窗口关闭、迟到判定都只使用**事件所属租户自己的水位线**；
- `currentWatermark()` 返回的“全局水位线”只是各租户水位线的最大值，仅用于观测，
  **不参与任何判定**。

> 设计要点：多租户共用时，安静（半天不来数据）的租户不会卡住别人，
> 跑得快的租户也不会误伤别人——见下方“租户间边界行为”。

### 窗口关闭与结果确定
窗口在其所属租户满足 `windowEnd + allowedLateness <= 该租户水位线` 时关闭并输出
`WindowResult`（count、sum、输出时水位线）。**只关闭该租户自己的窗口**；
其他租户水位线是否越过此边界完全无关。关闭后结果不可再变——任何属于该窗口的事件
此后必然被判定为超出允许迟到范围而拒绝，因此同一窗口不会产生重复或矛盾的结果。

### 迟到（Lateness）
- `eventTime < 该租户水位线 - allowedLateness`：以 `LATE_BEYOND_ALLOWED` **拒绝**，
  不静默丢弃；拒绝原因通过 `IngestOutcome.reason()` 可区分；
- 迟到但在允许范围内（`租户水位线 - allowedLateness <= eventTime < 租户水位线`）：
  正常接受并计入对应（尚未关闭的）窗口。

### 去重与幂等（Dedup / Idempotency）
- 相同 `eventId` 重复投递以 `DUPLICATE` 拒绝，不重复计数；
- **被任何原因拒绝（含额度满、背压）的事件都不会写入去重记录**，
  租户降速后重投此前被拒事件，不会被误判为 `DUPLICATE`；
- 去重记录容量有界（`maxDedupEntries`），超出后按插入顺序淘汰最旧条目，
  长期运行无无界状态增长；
- 重启后从历史重放：处理结论只取决于事件内容与顺序，全新实例按相同顺序
  重放得到与首次完全一致的窗口结果（见测试 `replayFromHistoryYieldsIdenticalResults`）。

### 背压（Backpressure）
- 在途事件 = 已接受但所属窗口尚未关闭的事件；
- 全局在途数达到 `maxInFlight` 时，新事件以 `BACKPRESSURE_CAPACITY` 拒绝；
- 豁免规则：若接受某事件所推进的水位线能关闭至少一个窗口（释放容量），
  即使在途已满也放行，避免“容量满 → 无法推进水位线 → 容量永不释放”的死锁。

### 租户级积压额度（Tenant Backlog Quota）
- 每个租户有独立的未确定积压额度 `tenantBacklog`（可通过构造参数给单个租户覆盖）；
- 某租户在途事件达到**自己的额度**时，只以 `TENANT_BACKLOG_LIMIT` 拒绝**该租户**，
  其他租户照常写入、照常定稿窗口，不被拖累；
- 拒绝原因与“迟到”（`LATE_BEYOND_ALLOWED`）、“重复”（`DUPLICATE`）、
  “全局背压”（`BACKPRESSURE_CAPACITY`）明确分开；
- 与全局背压一样：被拒事件**不**进入去重记录，降速重投不会被误判为重复；
- 同样有“推进水位线即可关闭本租户窗口”的豁免，避免单租户自死锁；
- 检查顺序：合法性 → 迟到 → 重复 → 租户额度 → 全局容量。

### 租户间边界行为（安静租户不互相干扰）
- **安静租户不卡别人**：A 租户的窗口能否定稿，只取决于 A 自己的数据是否已推进到
  `windowEnd + allowedLateness`；B 即使长时间停在很早的事件时间，也不影响 A 出结果。
- **快跑租户不误伤别人**：A 把水位线推得再靠前，也不会让 B 的正常（早到）数据
  被判成 `LATE_BEYOND_ALLOWED`，更不会替 B 关闭窗口。
- 同一租户内仍是标准多分区语义：租户水位线取其各分区最小值，
  自己某个安静分区只影响自己。

### 有界状态（长期运行不无限增长）
- 去重记录：`maxDedupEntries`，LRU 淘汰最旧 ID；
- 全局/租户在途事件：受全局上限与租户额度双重约束；
- 结果缓冲：保留最近 1024 条窗口结果；
- **租户数**：`maxTenants`（默认 1024）。满员且有新租户时，按最近访问顺序淘汰
  最久没有任何写入的租户（活跃写入中的租户永远在最近端，不会被淘汰）；
  淘汰时其残留的未定稿窗口与全局在途计数一并清理，快照仍自洽。被淘汰租户若再
  出现按“重新开始”处理；
- **每租户分区数**：`maxPartitionsPerTenant`（默认 1024），超限时淘汰最旧分区；
  这只会抬高分区最小值，租户水位线保持单调不减。被淘汰分区再来的过旧数据会以
  `LATE_BEYOND_ALLOWED` 明确拒绝。

### 并发与隔离
- 所有状态迁移在同一把锁内完成，支持多租户多分区并发写入；
- 进度快照也在同一把锁内生成：全局在途/窗口/拒绝计数恒等于各租户明细之和。

## 拒绝原因一览

| 原因 | 含义 |
|---|---|
| `INVALID` | 字段非法（空 ID、负时间戳等） |
| `LATE_BEYOND_ALLOWED` | 事件时间早于**该租户** `watermark - allowedLateness` |
| `DUPLICATE` | 相同 `eventId` 已处理过 |
| `BACKPRESSURE_CAPACITY` | 全局在途事件数达到 `maxInFlight` |
| `TENANT_BACKLOG_LIMIT` | 该租户自身积压额度已满，只挡该租户 |

## 使用示例

```java
// 窗口 1s、允许迟到 500ms、全局在途上限 10000、默认租户额度 1000、去重容量 10000
EventStreamProcessor p = new EventStreamProcessor(
        1_000L, 500L, 10_000, 1_000, 10_000);

// 也可给个别租户单独配置额度（其余租户用默认值），并显式设定有界上限：
//   new EventStreamProcessor(1_000, 500, 10_000, 1_000, 10_000,
//           Map.of("heavyTenant", 100), 1024, 1024);

IngestOutcome outcome = p.ingest(new Event("id-1", "tenantA", 0, 1_700_000_000_000L, 42));
if (!outcome.isAccepted()) {
    // 按 outcome.reason() 区分：LATE / DUPLICATE / TENANT_BACKLOG_LIMIT / BACKPRESSURE
}
List<WindowResult> results = p.finalizedResults();  // 已确定的窗口结果
ProgressSnapshot snapshot = p.progressSnapshot();   // 按租户/分区的进度快照
```

### 处理进度快照

`ProgressSnapshot` 包含：

- 全局：观测水位线、未定稿窗口总数、在途总数、按原因分类的拒绝总数；
- 每个租户（`TenantProgress`）：租户水位线、未定稿窗口数、在途事件数、
  按原因分类的拒绝数、各分区已见最大事件时间（`PartitionProgress` 列表）。

并发写入时，同一份快照里的全局汇总与各租户、各分区明细始终自洽
（全局在途 = Σ 各租户在途；全局窗口数 = Σ 各租户窗口数）。

## HTTP 查询入口（从进程外核对）

应用启动后：

```bash
# 全量进度快照
curl -s http://localhost:8080/api/stream/progress | jq .

# 只看某个租户
curl -s 'http://localhost:8080/api/stream/progress?tenant=tenantA' | jq .

# 写一条事件（同步返回判定，便于手工验证）
curl -s -X POST http://localhost:8080/api/stream/events \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","tenant":"tenantA","partition":0,"eventTimeMillis":100,"amount":42}'
```

额度、窗口、有界上限均可通过 `application.properties`/环境变量调整，键名见
`StreamConfig`（`stream.window-size-millis`、`stream.allowed-lateness-millis`、
`stream.max-in-flight`、`stream.tenant-backlog`、`stream.max-dedup-entries`、
`stream.max-tenants`、`stream.max-partitions-per-tenant`）。

## 验证方法

```bash
mvn test
```

测试覆盖：

| 测试类 | 验证点 |
|---|---|
| `EventStreamProcessorTest` | 原有乱序聚合、去重、迟到、窗口边界、全局背压、重放一致、多分区并发、非法事件、状态有界；其中 `tenantStatesAreIsolated` 已更新为租户级水位线语义（安静租户不被别人关窗、自己推进才定稿） |
| `MultiTenantProcessorTest.oneTenantBacklogCapBlocksOnlyItselfAndOthersKeepProgressing` | 某租户额度满只挡自己；被拒事件（含立刻重投）不判 DUPLICATE；其他租户照常推进定稿；降速后重试成功 |
| `MultiTenantProcessorTest.quietTenantDoesNotBlockOrRejectOtherTenants` | 安静/停写租户不卡其他租户出结果；快跑租户不把其他租户正常数据判晚、不替别人关窗 |
| `MultiTenantProcessorTest.concurrentMultiTenantWritesProduceSelfConsistentSnapshots` | 多租户多分区并发写入 + 并发读快照，全局/租户/分区计数自洽，拒绝总数与调用方观察一致 |
| `MultiTenantProcessorTest.rejectedByLateOrDuplicateAreCountedDistinctly` | 迟到、重复、额度满三类拒绝按租户分别计数 |
| `MultiTenantProcessorTest.tenantAndPartitionTrackersAreBounded` | 租户数、每租户分区数有界；LRU 淘汰后全局在途/窗口计数仍自洽 |
| `ProgressEndpointTest` | 从进程外经 HTTP 写事件、读全量/按租户过滤的进度快照 |

单测日志（INFO 级）会打印每条事件的接受/拒绝判定及依据（含租户额度与全局在途值）、
租户水位线推进（`tenantWatermark=old->new`）、窗口关闭结果
（`window FINALIZED tenant=.. count=.. sum=..`）以及有界淘汰（`.. EVICTED ..`），
可直接用于核对判定过程。
