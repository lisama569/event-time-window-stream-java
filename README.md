# event-time-window-stream-java

进程内本地实时事件流处理能力：以进程内状态模拟事件接入、事件时间窗口聚合与结果输出，
保证乱序到达、重复投递与并发写入下各入口结论一致且可解释；支持**多租户**并发使用：
按租户隔离积压额度、按租户独立推进水位线，并可从进程外（HTTP）随时查询处理进度快照。

## 核心概念

### 事件时间（Event Time）
每条 `Event` 携带 `eventTimeMillis`，窗口归属只由事件时间决定，与到达时刻无关：

```
windowStart = floor(eventTime / windowSize) * windowSize   // 窗口 [start, start+size)
```

窗口按租户（`tenant`）隔离：不同租户即使窗口边界相同，聚合状态也互不串扰。

### 水位线（Watermark）——按租户独立
- 每个租户**各自**跟踪自己每个分区的已见最大事件时间；
- **租户水位线 = 该租户各分区最大事件时间的最小值**，只增不减；
- 判定迟到、关闭窗口时，只使用**事件所属租户自己的水位线**。

这带来两个边界保证：

1. **安静租户不拖累别人**：某租户半天不来数据、事件时间停在很早的位置，
   只会停住它自己；其他租户只要自己的数据推进到足以确定窗口，其窗口照常定稿输出，
   不必干等慢租户跟上。
2. **别人推进不冤枉本租户**：另一个租户把事件时间跑到很远，**不会**把本租户的
   正常早到数据判成“太晚”——迟到判定只看本租户自己的水位线。

> 说明：旧版本是“跨所有分区取全局最小”的单水位线；升级为按租户独立水位线后，
> 一个租户的推进不再代替其他租户定稿窗口（见测试 `tenantStatesAreIsolated`）。

### 窗口关闭与结果确定
窗口在 `windowEnd + allowedLateness <= 该租户水位线` 时关闭并输出 `WindowResult`
（count、sum、输出时水位线）。关闭后结果不可再变——任何属于该窗口的事件
此后必然被该租户判定为超出允许迟到范围而拒绝，因此同一窗口不会产生重复或矛盾的结果。
已输出结果在进程内保留最近 1024 条，供轮询核对。

### 迟到（Lateness）
- `eventTime < 该租户水位线 - allowedLateness`：以 `LATE_BEYOND_ALLOWED` **拒绝**，
  不静默丢弃；拒绝原因通过 `IngestOutcome.reason()` 可区分；
- 迟到但在允许范围内（`watermark - allowedLateness <= eventTime < watermark`）：
  正常接受并计入对应（尚未关闭的）窗口。

### 去重与幂等（Dedup / Idempotency）
- 相同 `eventId` 重复投递以 `DUPLICATE` 拒绝，不重复计数；
- **只有被真正受理的事件才会进入去重表**。被额度/迟到等原因拒绝的事件不记录去重，
  因此“降速后重投”不会反而被判成重复（见 `tenantQuotaRejectedWriteIsNotMarkedDuplicateAndCanBeRetried`）；
- 去重记录容量有界（`maxDedupEntries`），超出后按插入顺序淘汰最旧条目。

### 租户级积压额度（Tenant Backlog Quota）
- 每个租户有自己的“未定稿事件积压”（在途事件）上限，可用 `TenantQuotas`
  按租户单独配置，未配置的租户使用默认额度；
- 某租户写得太猛、顶到**它自己**的上限时，仅以 `TENANT_BACKLOG_FULL` 挡下该租户，
  其他租户该写写、该出结果出结果，**完全不受拖累**；
- 该拒绝原因与 `LATE_BEYOND_ALLOWED`（太晚）/ `DUPLICATE`（重复）明确区分；
- 被拒事件不进过去重表，租户降速、容量随窗口定稿释放后即可重投成功；
- 豁免规则：若接受某事件所推进的**该租户**水位线能关闭至少一个窗口（释放容量），
  即使该租户额度已满也放行，避免“额度满 → 无法推进水位线 → 容量永不释放”的死锁；
- 另有全进程在途总量安全上限（`globalInFlight`），超过时以 `BACKPRESSURE_CAPACITY`
  拒绝，仅用于进程内存保护，默认远高于单租户额度。

### 状态有界（长期运行不无限增长）
所有新增额度与计数都是有界的：

| 状态 | 上界 |
|---|---|
| 去重条目 | `maxDedupEntries`（LRU 淘汰） |
| 驻留租户数 | `maxTenants`（名额满时回收安静/长期空闲租户） |
| 每租户分区数 | `maxPartitionsPerTenant` |
| 每租户在途事件 | 该租户额度 `TenantQuotas` |
| 全进程在途事件 | `globalInFlight` |
| 已输出结果缓冲 | 最近 1024 条 |
| 拒绝计数的“键” | 拒绝原因是有限枚举，不会随事件增长 |

租户回收策略：名额已满且有新租户到来时，优先回收**完全安静**（无在途、无未定稿窗口）
的租户；若没有安静租户，则回收距上次活动超过 `idleTenantEvictMillis` 的最久未活动租户。
已确定的窗口结果在回收前已输出到结果缓冲，回收不丢已定稿结果；其按租户的拒绝计数随之清退，
而**全局累计拒绝计数保留**。

### 并发与隔离
- 所有状态迁移在同一把锁内完成，支持多租户多分区并发写入；
- 快照也在同一把锁内复制生成，因此并发写入下读到的各计数彼此自洽。

## 拒绝原因一览

| 原因 | 含义 | 是否记录去重 |
|---|---|---|
| `INVALID` | 字段非法（空 ID、空租户、负时间戳、分区数超限等） | 否 |
| `LATE_BEYOND_ALLOWED` | 事件时间早于该租户 `watermark - allowedLateness` | 否 |
| `DUPLICATE` | 相同 eventId 已被真正受理过 | —（本身即去重命中） |
| `TENANT_BACKLOG_FULL` | 该租户未定稿积压达到它自己的额度 | 否（可降速重投） |
| `BACKPRESSURE_CAPACITY` | 全进程在途总量达到安全上限 | 否 |

## 处理进度快照（进程外可查询）

启动应用后可直接 HTTP 查询：

- `GET /api/progress`：全部租户；
- `GET /api/progress?tenant=<租户名>`：只看指定租户。

快照内容（在同一把锁内生成，并发下自洽）：
- `globalWatermark`：各活跃租户水位线的最小值（观测用）；
- `totalInFlight` / `activeTenants`：全进程在途总数、活跃租户数；
- `globalRejectionCounts`：到目前为止按原因分类的**全局**拒绝条数；
- 每个租户：`watermark`、`inFlight`、`quota`、`openWindowCount`（未定稿窗口数）、
  各 `partitions` 的当前最大事件时间，以及该租户自己的分原因 `rejectionCounts`。

另有事件接入入口，便于从进程外灌数与核对：

- `POST /api/events`，JSON：`eventId/tenant/partition/eventTimeMillis/amount`；
- 受理返回 200；`INVALID` 返回 400，其余拒绝返回 429，响应体含 `status` 与 `reason`。

可配置项（均有默认值，见 `application.properties`）：

```
stream.window-size-millis=1000
stream.allowed-lateness-millis=500
stream.default-tenant-inflight=10000
stream.global-inflight=1000000
stream.max-dedup-entries=100000
stream.max-tenants=128
stream.max-partitions-per-tenant=1024
stream.idle-tenant-evict-millis=60000
stream.tenant-quota.<租户名>=<额度>   # 例如 stream.tenant-quota.flooder=2
```

## 使用示例

```java
// 简单场景：所有租户共用默认额度（等价于旧的 4 参构造）
EventStreamProcessor p = new EventStreamProcessor(
        1_000L,    // 窗口长度 1s
        500L,      // 允许迟到 500ms
        10_000,    // 每个租户未定稿积压默认上限
        10_000);   // 去重容量

// 多租户场景：flooder 单独小额度，其他租户走默认额度
EventStreamProcessor multi = new EventStreamProcessor(
        1_000L, 500L,
        new TenantQuotas(10_000, Map.of("flooder", 2)), // 租户级额度覆盖
        1_000_000,                                     // 全进程在途安全上限
        100_000,                                       // 去重容量
        128, 1_024, 60_000L,                           // 最大租户数/每租户分区数/空闲回收ms
        System::currentTimeMillis);                    // 活动时钟（测试可注入固定时钟）

IngestOutcome outcome = multi.ingest(new Event("id-1", "tenantA", 0, 1_700_000_000_000L, 42));
if (!outcome.isAccepted()) {
    // 按 outcome.reason() 区分：TENANT_BACKLOG_FULL 就降速重投；LATE 归入死信；DUPLICATE 忽略
}
ProgressSnapshot snap = multi.progressSnapshot();      // 全部租户
ProgressSnapshot one  = multi.progressSnapshot("tenantA");
```

## 验证方法

```bash
mvn test
```

测试覆盖：

| 测试类 | 测试 | 验证点 |
|---|---|---|
| `EventStreamProcessorTest` | `outOfOrderEventsAggregateIntoSameWindowAndFinalizeOnWatermark` | 乱序事件归入正确窗口 |
| | `duplicateDeliveryIsRejectedAndCountedOnce` | 重复投递拒绝且只计一次 |
| | `lateBeyondAllowedIsRejectedWithDistinguishableReason` | 超迟到原因可区分 |
| | `windowBoundaryEventBelongsToNextWindow` | 窗口边界归属 |
| | `backpressureRejectsWhenInFlightFullAndRecoversAfterFinalize` | 租户额度拒绝、可重试、豁免 |
| | `tenantStatesAreIsolated` | 租户水位线/窗口独立，安静租户不被别人带着定稿 |
| | `replayFromHistoryYieldsIdenticalResults` | 重放结论一致 |
| | `concurrentMultiPartitionWritesAreConsistent` | 多分区并发一致 |
| | `invalidEventsAreRejected` | 非法事件以 INVALID 拒绝 |
| | `dedupStoreIsBounded` | 长期运行状态有界 |
| `MultiTenantEventStreamProcessorTest` | `oneTenantHittingQuotaDoesNotBlockOthers` | 某租户顶额度只挡自己，别人照常定稿 |
| | `quietTenantNeitherBlocksOthersNorGetsItsEarlyDataRejected` | 安静租户不卡别人、也不被别人误伤 |
| | `progressSnapshotIsSelfConsistentUnderConcurrentWrites` | 并发写入下按租户/分区快照自洽 |
| | `tenantQuotaRejectedWriteIsNotMarkedDuplicateAndCanBeRetried` | 额度拒绝后重投不误判重复 |
| | `idleQuiescentTenantsAreEvictedSoStateStaysBounded` | 租户数有界、安静租户被回收且结果保留 |
| `ProgressEndpointTest` | `progressSnapshotReadableOverHttpAndReflectsState` | 进程外 HTTP 读到自洽快照与租户额度 |

单测日志（INFO 级）打印每条事件的接受/拒绝判定及依据（含 `reason=`、
`tenantInFlight=used/quota`）、租户水位线推进（`tenantWatermark=old->new`）
与窗口关闭结果（`window FINALIZED tenant=.. count=.. sum=..`），可直接核对判定过程。

### 本地手动冒烟（进程外）

```bash
# 启动并把 flooder 的额度调小
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=18080 --stream.tenant-quota.flooder=2"

# 另开终端：灌数据
curl -X POST localhost:18080/api/events -H 'Content-Type: application/json' \
  -d '{"eventId":"f1","tenant":"flooder","partition":0,"eventTimeMillis":100,"amount":1}'
# flooder 写到第 3 条 -> 429 {"reason":"TENANT_BACKLOG_FULL",...}；降速重投不会变 DUPLICATE

# 随时查看进度快照（按租户/分区的事件时间、未定稿窗口数、分原因拒绝数）
curl -s localhost:18080/api/progress | python3 -m json.tool
curl -s "localhost:18080/api/progress?tenant=flooder" | python3 -m json.tool
```
