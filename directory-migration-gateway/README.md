# 目录运维迁移网关（在线切换 + 按 URL 熔断）

运维迁移制品目录服务时，网关需要**在线切换上游目录、随时可切回**，并且切换过程不能
影响在途查询、不能让迟到结果污染新配置的熔断结论。本项目用 **Python 标准库**实现
（零第三方依赖），提供：

* 两个可分别控制**延迟 / 503** 的目录实例（`directory-a`、`directory-b`），元数据响应
  中带可配置的**目录标识** `directory.id`；
* 双 worker 网关：切换在瞬间完成，**不等待旧慢查询**；新受理查询立即走新目录；
* 切换前对目标 URL 做 `example-app.json` 预检，必须成功且目录标识与预期相符，
  **预检不经过、也不改变任何熔断计数**；
* 乐观版本号切换：同版号并发切换只有一个赢家，其余得到 409；预检失败、写入失败、
  版本冲突时**配置保持不变**；
* 熔断状态**按 URL 各自独立**，切回不清零；配置与熔断状态落盘，**重启后保留**，
  有状态卷时原目录的熔断状态不丢。

## 快速启动（Docker Compose）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8080/
```

| 服务 | 说明 | 端口 |
| --- | --- | --- |
| `directory-a` | 目录实例 A（标识 `directory-a`），可控延迟 / 503 | 仅集群内 9000 |
| `directory-b` | 目录实例 B（标识 `directory-b`），可控延迟 / 503 | 仅集群内 9000 |
| `gateway` | 双 worker 网关，状态写入命名卷 `gateway-data:/data` | 宿主机 **8080** |

镜像基于 `python:3.11-slim`，构建与运行时均不执行 pip 安装。

### 不用 Docker 的本地运行

```bash
./run_local.sh                 # 起三进程并自动跑完整自测（含一次网关重启）
./run_local.sh --keep          # 自测后保留服务，浏览器访问 http://127.0.0.1:8080/
```

## 浏览器里能做什么

打开网页后可：

1. **查看当前配置**：版号、当前上游 URL、默认制品、最近切换时间；
2. **在线切换**：填目标 URL、预期目录标识、所见配置版号；可先点“仅预检”，
   也可直接“预检并切换”；
3. **发起查询**：可选并发次数，结果展示命中的 worker、绑定的上游 URL、
   返回的目录标识、熔断事件以及是否为被隔离的陈旧（stale）结果；
4. **分别控制两个目录实例**的 `normal / delay / 503` 模式与延迟秒数、清零访问计数；
5. **查看各 URL 熔断状态**（状态、连续失败、累计请求/失败/拒绝、era/generation、
   最近事件）与两个 worker 的在飞查询；页面每 2 秒自动刷新。

## 关键语义与实现方式

### 切换流程（`POST /api/switch`）

1. 校验 URL / 标识 / 版号；
2. **预检**：直接请求 `目标URL/artifacts/example-app.json`，要求 HTTP 200 且响应
   `directory.id` 等于预期标识。预检走独立短超时路径，**不调用熔断器、不改计数**；
3. 持锁做**乐观提交**：内存版号 ≠ 提交版号 → 409 版本冲突（同版号并发切换的败者）；
4. 配置以「临时文件 + fsync + 原子 rename」落盘；**写入失败时内存配置也不改**，
   返回 500 并显式标注 `config_unchanged: true`；
5. 提交成功后给目标 URL 的熔断记录 `era + 1`，使该 URL 上所有切换前已在飞的结果
   全部成为陈旧结果。

### 双 worker 与“不等待旧慢查询”

查询进入有界队列，两个 worker 线程在**出队受理时**读取当时的配置，并把本次调用
钉在（绑定）该 URL、配置版号、era、generation 上。切换只改配置对象，不触碰队列中的
作业，因此：

* 切换瞬间完成，不等待任何在途慢查询；
* 在途旧查询继续访问旧目录，成功/失败后**照常返回原结果**，响应里注明
  `upstream_url`、`config_version` 与目录标识；
* 切换后新受理的查询立即绑定新 URL。

### 迟到结果不能改变新配置的熔断结论

* 每个 URL 的熔断记录带 `era`（被选为上游的代次）与 `generation`（状态机变迁代次）；
* 调用完成回写时，若 `era` 或 `generation` 已不匹配，结果记为
  `stale_success_ignored / stale_failure_ignored`，**不改变任何计数与状态**，
  HTTP 响应里带 `stale_result_ignored: true`；
* 若一个 HALF_OPEN 恢复探测在飞时该 URL 被切走又切回，重选会把无主探测槽释放并
  重新 OPEN + 重新冷却，避免探测槽永久卡死。

### 按 URL 隔离的熔断器

状态机 `CLOSED → OPEN → HALF_OPEN → CLOSED/OPEN`：

* 连接失败 / 超时 / 5xx 计失败；2xx/3xx/**4xx** 视为目录健康（4xx 清零计数）；
* 连续失败达到阈值（默认 3）打开；OPEN 期间请求直接 503，**不访问目录**；
* 冷却（默认 5s）后恰好一个请求成为恢复探测（锁内选举，双 worker 下也唯一）；
* 每个 URL 独立记录，切换/切回都**不会清零**；重置接口只清计数、不删 URL 记录。

### 持久化与重启

* `data/config.json`：当前上游 URL、版号等；`data/breakers.json`：全部 URL 的熔断状态；
* 原子写 + 目录 fsync；启动时兼容带 `{"breakers": ...}` 包装与裸映射两种历史格式；
* Compose 中 `/data` 是命名卷，容器重启/重建后配置与各 URL（含原目录）熔断状态都在。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/`、`/static/app.js` | 网页与前端脚本 |
| GET | `/healthz` | 健康检查（含当前配置摘要） |
| GET | `/api/config` | 当前配置（版号、上游 URL、制品） |
| GET | `/api/query?artifact=` | 经 worker 池查询目录 |
| GET/POST | `/api/preflight` | 预检目标 URL 标识，不碰熔断计数 |
| POST | `/api/switch` | 预检 + 乐观版号切换 |
| GET | `/api/breakers` | 各 URL 熔断状态 |
| POST | `/api/breakers/reset` | 清零熔断计数（保留 URL 记录） |
| GET | `/api/inflight` | worker 数与在飞查询（绑定 URL/版号/耗时） |
| GET | `/api/directories` | 两个目录实例的模式、标识、真实访问计数 |
| POST | `/api/directories/control` | 代理目录控制：`mode` / `directory-id` / `stats-reset` |

目录实例自身还暴露 `GET/POST /control/mode`、`POST /control/directory-id`、
`GET /control/state`、`GET /control/stats`、`POST /control/stats/reset`、`/healthz`。

## 自测

```bash
python3 -I tests/test_unit.py     # 纯单元测试：状态机/持久化/切换冲突/预检校验
./run_local.sh                    # 端到端 46 项检查（自动起停三进程并重启网关一次）
```

端到端覆盖：基线查询与目录标识、503 三连失败只打开本 URL 的熔断器、预检不计数、
OPEN 本地拒绝且不访问目录、慢查询钉住旧 URL 且迟到成功/失败均被 era 隔离、
同版号并发切换唯一赢家、错误标识/不可达/写入失败时配置不变、切回保留原 URL
熔断状态、重启后配置与熔断状态（含 OPEN）完整保留。

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PORT` | 网关 8080 / 目录 9001 | 监听端口 |
| `DATA_DIR` | `./data` | 配置与熔断状态目录（Compose 挂命名卷 `/data`） |
| `UPSTREAM_URL` | `http://directory-a:9000` | 初始上游（仅首次启动无配置文件时生效） |
| `DIRECTORY_A_URL` / `DIRECTORY_B_URL` | 集群内域名 | 网页控制面板对应的两个实例 |
| `WORKER_COUNT` | `2` | 查询 worker 线程数 |
| `REQUEST_TIMEOUT` | `8.0` | 单次查询总超时（秒） |
| `PREFLIGHT_TIMEOUT` | `5.0` | 预检超时（秒） |
| `COOLDOWN` | `5.0` | 熔断冷却（秒） |
| `FAILURE_THRESHOLD` | `3` | 连续失败打开阈值 |
| `DIRECTORY_ID`（目录） | `directory-default` | 目录实例对外标识，可经控制接口运行时修改 |
