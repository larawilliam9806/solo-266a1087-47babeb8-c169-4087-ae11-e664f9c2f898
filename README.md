# 档案分页采集台

给档案数据员用的浏览器采集台：粘贴上游接口地址，后台分页抓取，实时看进度，
随时暂停/继续，完成后下载按记录 ID 去重的 JSONL。附带可切换故障的本地演示源。

- 后端：Flask 3 + SQLite（检查点、记录、事件全部落盘）
- 前端：单页原生 JS（轮询刷新，无构建步骤）
- 部署：Docker Compose（采集台 + 演示源两个容器）

## 快速启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 采集台（浏览器） | http://localhost:8000 |
| 演示源直连 | http://localhost:9000/records |

打开 http://localhost:8000 ，输入框已预填 `http://demo:9000/records`，
点击「启动任务」即可。采集台通过 `/demo/*` 同源代理控制演示源，页面上的
「慢响应 / 重复记录 / 429 限流」按钮可随时切换故障。

> `http://demo:9000/records` 只在 compose 网络内可解析，所以这是容器里默认预填的
> 地址。若采集台在宿主机本地运行（非 Docker），表单会预填
> `http://127.0.0.1:9000/records`。

数据卷 `collector-data` 挂到 `/data`，容器重建后进度仍在。

### 不使用 Docker 的本地运行

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m demo.upstream &                      # 演示源 :9000
COLLECTOR_DATABASE=./instance/collector.sqlite3 \
  python3 wsgi.py                               # 采集台 :8000
```

测试：`python3 -m pytest tests -q`（约 55 秒，含真实 HTTP 慢响应/429 场景）。

## 功能说明

### 接口契约

上游每页返回：

```json
{"records": [{"id": "rec-0001", "body": "..."}], "next_cursor": "1"}
```

`next_cursor` 为空/null 表示最后一页；后续请求追加 `?cursor=<值>`（原地址已有
查询参数时用 `&`）。同一记录 ID 的正文由上游保证一致。

### 去重与持久化

- `records` 表主键为 `(job_id, record_id)`，插入用 `INSERT OR IGNORE`，
  重复记录只计数不落库、不进下载文件。下载按首次出现顺序输出。
- 每一页的记录与“下一页游标”在**同一个 SQLite 事务**里提交。所以任何时刻
  崩溃，磁盘上都是“已完整提交的若干页 + 指向下一页的游标”。

### 暂停语义

暂停是**在数据库里同步确认**后才返回 200 的：状态置 `paused` 并递增
`run_epoch`（围栏令牌）。采集线程在写库时必须在同一事务内校验
「状态仍是 running 且 epoch 未变」，否则整页丢弃、不写记录、不改进度、
不动游标。因此暂停不需要等在途请求结束，在途响应即使返回也不会产生副作用。
前端轮询会立即显示「已暂停」。

### 继续与重启恢复

- 继续时读取数据库里保存的游标和最近一页之后的位置，从该游标重新请求。
- 进程重启后 `wsgi.py` 启动时把所有遗留的 `running` 任务改成 `paused`
  （并记录事件「服务重启，任务已暂停；请手动继续」），必须手动点「继续」。
- 重试同一页最多造成重复抓取，不会漏记录、不会重复收录、不会让游标回退：
  游标只在提交成功时前进，且提交前校验游标仍等于本次请求发出的游标。
- 单次运行页数上限由 `MAX_PAGES` 控制（默认 100000）。

### 429 与重试

- 429：读 `Retry-After` 头（秒），上限 `MAX_RETRY_AFTER`（默认 300s），
  等待期间界面显示「429 限流：正在按 Retry-After 等待…」。
- 等待被切成 200ms 片段，每片都检查暂停标志，所以**等待中仍可立即暂停**。
- 5xx / 408 / 425 / 网络错误：指数退避重试最多 6 次；仍失败则任务进入
  `error` 状态，事件日志写明原因。
- 非 200 但非可重试的状态码（如 404）直接使任务失败并在界面标红。

### 下载

`GET /api/jobs/<id>/download` 流式输出 `application/x-ndjson`，文件名
`archive-<jobId前12位>.jsonl`，每行一条 `{"id":..., "body":...}`，已按 ID 去重。

## 演示源故障开关

| 开关 | 效果 |
| --- | --- |
| 慢响应 1.5s | 每次请求延迟 1.5s，用于演示“暂停无需等待在途请求” |
| 重复记录 | 每页把上一页记录原样再发一次（同 ID 同正文），验证去重 |
| 429 限流 | 每 3 次请求返回一次 429 + `Retry-After: 3`，等待期可暂停 |
| 恢复默认 | 关闭所有故障注入并清空请求日志 |

数据固定 23 条、每页 5 条（5 页，最后一页 3 条），便于核对条数与顺序。
也可直接调用 `POST /demo/config` 精细控制
（`slow_ms` / `duplicates` / `rate_limit_every` / `retry_after`），
`GET /demo/stats` 可查看每个请求的游标、是否被限流、何时完成。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 采集台页面 |
| POST | `/api/jobs` | `{"url": "..."}` 新建并启动任务 |
| GET | `/api/jobs` | 任务列表 |
| GET | `/api/jobs/<id>` | 单任务详情 + 事件日志 + `server_time` |
| POST | `/api/jobs/<id>/pause` | 确认暂停（不等在途请求） |
| POST | `/api/jobs/<id>/resume` | 从保存的游标继续 |
| DELETE | `/api/jobs/<id>` | 删除任务及其数据 |
| GET | `/api/jobs/<id>/download` | 下载去重 JSONL |
| GET | `/healthz` | 健康检查 |

## 结构

```
app/__init__.py    应用工厂/配置
app/db.py          SQLite 结构、事务提交、围栏令牌、重启恢复
app/collector.py   采集线程、429/退避重试、可中断等待
app/routes.py      HTTP 接口与下载
app/demo_proxy.py  /demo/* 同源代理到演示源
app/templates/     单页界面
app/static/        样式与前端逻辑
demo/upstream.py   本地演示源（故障注入）
tests/             12 个功能测试
wsgi.py            启动入口（建表 + 恢复遗留任务）
```

## 已知限制

- 采集线程运行在 Web 进程内。因此 gunicorn 固定 `--workers 1 --threads 16`：
  多进程时暂停 Event 只能通知到其中一个进程（数据库围栏仍能保证不写错数据，
  但被落下的进程要等到当前请求结束才会退出，状态会短暂不一致）。
  若要多副本，需要把 worker 拆成独立进程 + 队列。
- 进度条在未知总页数时只表示“活跃度”，真实进度以“已采记录 / 已完成页”数字为准。
- 演示源的数据集是内存中固定的 23 条，不持久化故障开关。
- `DELETE` 对正在运行的任务会先确认暂停再删除；在途响应因外键级联/围栏检查
  被丢弃，不会重建数据。
