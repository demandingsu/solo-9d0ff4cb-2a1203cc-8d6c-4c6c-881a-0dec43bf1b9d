# 双端 FASTQ 增量采集服务（FastAPI + SQLite）

测序中心数据员从仪器中转站接收持续追加的双端读段（`R1.fastq` / `R2.fastq`），
按读段名配对后交给下游比对。本项目提供：

- **采集端（collector）**：FastAPI + SQLite，通过 HTTP Range 增量拉取、
  四行 FASTQ 校验、按名称配对、字节级断点续采，全部配对完成后可下载 R1/R2；
  **未配对读段写入 SQLite（`pending` 表）而非堆在内存**，因此在受限内存
  下也能承受"R1 领先数万条、R2 尚未写入"的单端超前；
- **仪器中转站（relay）**：本地模拟服务，支持 HTTP Range、分段追加、
  单端大幅滞后（`r2_lag_reads` / `r2_start_delay`）、完成标志 `DONE`
  与可控制的连接中断窗口；打开 `/docs` 可交互式演示；
- **一键演示（demo）**：构造 30000 条 R1 领先、R2 延迟到达的场景，反复
  触发采集、下载并逐字节校验。

## 目录结构

```
.
├── collector/            # 采集端
│   ├── app.py            # REST API（采集/进度/下载/健康检查）
│   ├── service.py        # Range 续采 + 磁盘暂存 + 配对状态机 + 终态校验
│   ├── parser.py         # 字节级 FASTQ 解析与结构校验
│   └── db.py             # SQLite 模式、pending 暂存与单事务配对提交
├── relay/                # 模拟仪器中转站
│   └── app.py            # Range 文件服务 + 追加写线程 + 单端滞后 + 中断控制
├── scripts/demo.py       # 端到端演示脚本（大 R1 领先场景）
├── tests/                # 解析单元测试 + 真实子进程集成测试 + 单端超前专项
├── Dockerfile            # 通用镜像（APP_MODULE 选择入口）
├── docker-compose.yml    # relay / collector / demo 三服务
└── requirements*.txt
```

## 快速开始（Docker Compose）

```bash
# 启动两个常驻服务（collector 在 8000，relay 在 8001）
docker compose up -d --build

# 运行端到端演示：默认 30000 条 R1 领先、R2 延迟到达、含一次中断
docker compose --profile demo run --rm demo

# 想更狠地压测受限内存（例如 256MB 上限）：
COLLECTOR_MEM_LIMIT=256m DEMO_READS=80000 docker compose --profile demo run --rm demo

# 查看日志
docker compose logs -f collector
```

### 通过 `/docs` 演示"大量 R1 领先、R2 延迟到达"

浏览器打开 <http://localhost:8001/docs>：

1. 调用 `POST /demo/large-lead`（可设 `reads`、`seq_len`），它会创建一个
   R1 先全部落盘、R2 延迟 3 秒才开始追赶的场景，并返回
   `absolute_base_url`（如 `http://localhost:8001/runs/lead-171...`）；
2. 打开 <http://localhost:8000/docs>，调用 `POST /collect/runs`，body 为
   `{"run_id": "<上一步 run_id>", "base_url": "<absolute_base_url>"}`；
3. 反复调用 `GET /collect/runs/{id}`：可看到 `r1_fetched` 与
   `r1_pending` 上升、`pairs` 保持 0；R2 追上后 `pairs` 快速上升且两端
   `r1_offset`/`r2_offset` 一起前推，最终 `status=completed`。

也可手动操作：

```bash
# 1) 在中转站创建一个 R1 大幅领先、R2 延迟到达、带连接中断的场景
curl -X POST http://localhost:8001/runs -H 'content-type: application/json' -d '{
  "run_id": "runA", "reads": 30000, "append_delay": 0.0,
  "r2_extra_delay": 0.0, "r2_lag_reads": 30000, "r2_start_delay": 3.0,
  "chunk": 1500, "interrupt_at_second": 0.4, "interrupt_duration": 1.5,
  "seq_len": 120
}'

# 2) 提交采集（可任意重复调用，幂等）
curl -X POST http://localhost:8000/collect/runs -H 'content-type: application/json' \
  -d '{"run_id": "runA", "base_url": "http://relay:8000/runs/runA"}'

# 3) 查询进度（r1_fetched 先涨、r1_pending 暂存领先读段，pairs 待 R2 追上后增长）
curl http://localhost:8000/collect/runs/runA

# 4) 全部配对（status=completed）后下载
curl -OJ http://localhost:8000/collect/runs/runA/R1
curl -OJ http://localhost:8000/collect/runs/runA/R2
```

> 在宿主机直接调用且不经过 compose 网络时，`base_url` 用
> `http://localhost:8001/runs/runA`。

## API 一览

### 采集端 `:8000`

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/collect/runs` | 提交/触发一次增量采集，body：`{"run_id","base_url"}`，幂等可重复 |
| GET | `/collect/runs` | 列出全部运行 |
| GET | `/collect/runs/{id}` | 查询状态、两端字节进度、配对数、错误详情 |
| GET | `/collect/runs/{id}/R1` `/R2` | 下载已配对 FASTQ，**仅 `completed` 允许**，否则 409 |
| GET | `/healthz` | 存活探针 |

进度响应示例：

```json
{
  "run_id": "runA",
  "base_url": "http://relay:8000/runs/runA",
  "status": "running",
  "r1_offset": 0,
  "r2_offset": 0,
  "r1_fetched": 4800000,
  "r2_fetched": 0,
  "pairs": 0,
  "r1_pending": 30000,
  "r2_pending": 0,
  "error": null,
  "notices": ["R1 源站连接中断（HTTP 503），保留断点 240000，等待重试"]
}
```

字段含义：

- `r1_offset` / `r2_offset`：**已配对**字节进度，同一配对事务中一起前推；
- `r1_fetched` / `r2_fetched`：**已落盘采集**进度，单端领先时会先于
  已配对进度前进；
- `r1_pending` / `r2_pending`：已落盘但尚未配对的记录条数（单端领先时
  只占磁盘，不占采集进程内存）。

状态取值：`running`（采集中/等待追加）、`completed`（全部配对）、
`error`（结构错误、名称不匹配、完成后残条或孤读）。

### 中转站 `:8001`

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/runs` | 按场景启动后台追加任务（延迟、块大小、`r2_lag_reads`、`r2_start_delay`、中断窗口均可配） |
| POST | `/demo/large-lead` | 一键创建"R1 大幅领先、R2 延迟到达"演示场景，返回可复制的 `absolute_base_url` |
| GET | `/runs/{id}/status` | 查看追加进度与中断窗口状态 |
| POST | `/runs/{id}/interrupt` | 手动打开中断窗口（body：`{"duration": 3}`） |
| POST | `/runs/{id}/resume` | 提前关闭中断窗口 |
| GET/HEAD | `/runs/{id}/R1.fastq` `/R2.fastq` `/DONE` | 文件服务，支持 `Range: bytes=a-b`（206/416） |

打开 <http://localhost:8001/docs> 可直接在 Swagger UI 里操作上述端点。

## 数据格式与配对规则

- 仅处理**未压缩** FASTQ，每条记录严格四行：
  `@名称` / 序列 / `+` / 质量串；
- R1 名称以 `/1` 结尾、R2 以 `/2` 结尾，**去掉后缀后名称一致**才配对；
- 序列与质量串必须等长，第三行必须以 `+` 开头，否则判定结构错误；
- 一条记录被读取窗口切开（甚至切在行中间）、暂时读到文件末尾或连接
  中断时**等待续采**，不会产生半对；
- 源站 `DONE` 出现后，必须两端都读到**真实末尾**（416/404 或读满
  `Content-Range` 报告的总长）才做终态校验；若仍有残条
  （`residual_records`）或孤读（`orphan_read`），进入 `error` 终态并回报
  两端字节位置；只有全部配对成功才置 `completed`，此时才允许下载。

## 单端超前采集：为什么不会 MemoryError

旧实现把已解析但尚未配对的 ``Record`` 放在内存队列里。R1 已有数万条、
R2 尚未写入时，整份 R1 都会堆在采集进程内存中，受限内存下直接
`MemoryError`。现在改为**磁盘暂存 + 有界内存**：

1. 每解析出一条完整记录，立即在单事务中写入 SQLite `pending` 表，并前推
   该端的 `r*_fetched`；
2. 采集进程每个端在内存里只保留**末尾凑不齐四行的残条**（`buffer`），
   大小受单条记录长度约束（默认上限 `COLLECTOR_MAX_PARTIAL`=64MiB，
   畸形输入也不会撑爆）；
3. 配对时按批（`COLLECTOR_PAIR_BATCH`，默认 500 条）从 `pending` 顺序
   读取，配成一对即提交并删除对应行，内存占用与积压量无关；
4. R2 追上后，按 `pending` 自增顺序（即源文件顺序）配对，**两端进度在
   同一事务中一起前推**。

实测：R1 领先 40000 条（约 12.6MB）时，采集进程峰值 Python 分配约
2.4MB，R2 追上后 40000 对全部配对且下载内容与源文件逐字节一致。

## 不漏对、不重对的设计保证

1. **两段式单事务提交边界**：
   - `append_pending`：记录落盘与该端采集进度前推同一事务；
   - `commit_pairs`：配对内容（`pairs`）、删除已消费 `pending`、两端
     **已配对偏移**与配对数同一事务。
   任一步失败整体回滚，不存在"内容落库但偏移没更新"或反之的中间态。
2. **Range 断点续采**：下一次请求起点 = 已落盘进度 `r*_fetched` + 内存残条
   长度；断线（503/连接重置/网络异常）只记录 `notices`，状态保持
   `running`，已落盘记录与残条保留，重连后从断点继续。
3. **重启恢复**：进程重启后内存状态清空，从 SQLite 的 `r*_fetched` 与
   `pending` 行恢复；已暂存的领先读段无需重拉，已配对记录因唯一约束
   （`UNIQUE(run_id,name)` + 顺序号主键）不会重复。
4. **完成前必须确认 EOF**：源站出现 `DONE` 后，只有两端都读到真实末尾
   且本轮无传输问题才做终态校验，避免把"还没拉到的完整记录"误判成残条/
   孤读。
5. **Range 一致性保护**：若源站在非零偏移处返回 200 而非 206（不支持
   Range，可能发生字节改写），直接进入 `error`，拒绝继续采集。
6. **旧库兼容**：`init_db` 会为已有 `runs` 表补齐 `r*_fetched` 列（抬到不
   小于已配对偏移），保留原有运行、配对数据与配对数。

## 测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

测试包含 26 个用例：解析器偏移/残行/结构校验单元测试，真实 uvicorn
子进程下的端到端集成测试——增量配对、R2 滞后、连接中断续采、
**采集进程重启后从 SQLite 断点恢复**、名称不匹配/结构错误/残条/孤读的
错误终态与两端位置回报、Range 206/416、下载门控；以及单端超前专项：
**数万条 R1 领先时只落盘不占内存**、记录跨读取窗口不被丢弃、领先读段在
重启后不重拉、DONE 出现后必须读到真实末尾才判孤读。

## 环境变量

| 变量 | 服务 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `COLLECTOR_DB` | collector | `/data/collector/collector.db` | SQLite 文件路径 |
| `COLLECTOR_TIMEOUT` | collector | `10` | 单次 HTTP 超时秒数 |
| `COLLECTOR_PAIR_BATCH` | collector | `500` | 单事务最大配对数（调节内存/写放大） |
| `COLLECTOR_MAX_PARTIAL` | collector | `67108864` | 单条残记录字节上限 |
| `RELAY_DATA` | relay | `/data/relay` | 运行目录根路径 |
| `RELAY_URL` / `COLLECTOR_URL` / `RUN_ID` | demo | 见 `scripts/demo.py` | 演示目标 |
| `READS` / `SEQ_LEN` | demo | `30000` / `120` | 演示数据规模 |
| `COLLECTOR_MEM_LIMIT` | compose | `1g` | collector 容器内存上限，调小可压测 |
| `DEMO_READS` / `DEMO_SEQ_LEN` | compose | `30000` / `120` | 透传给 demo 的规模 |
