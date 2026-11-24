# 虚拟卷在线换条带模拟（Flask + SQLite）

模拟一块由两块物理盘（每盘 16 槽、每槽 4 KiB）组成的 32 块虚拟卷，在**不停止读写**的
前提下把条带从“单块交替”改成“每 2 块 / 每 4 块换盘”。搬运逐批提交，支持在写槽后、
提交前让进程退出，重启后从最后一个已提交位置继续。

## 模型与规则

| 项目 | 实现 |
| --- | --- |
| 物理盘 | SQLite 表 `slots`：盘 0/1，各 16 槽，共 32 个数据槽，初始全部装满 4 KiB 块 |
| 初始条带 | 单块交替：LBN `l` 落在盘 `l%2`、槽 `l//2` |
| 新条带 | 选 `k=2` 或 `k=4`：LBN `l` 落在盘 `(l//k)%2`、槽 `(l//(2k))*k + l%k` |
| 临时槽 | 独立表 `staging`，同一时刻最多 1 块正文（题目要求的额外暂存上限） |
| 每批额度 | 表单填写“本批最多写几次盘槽正文”，**临时槽 stage/unstage 也计次**；额度 1 也执行一次搬运，必然推进 |
| 在线读写 | 任意阶段按 LBN 读写；块在临时槽时读/写都落到 `staging`，始终拿到最近一次已提交正文 |
| 值班员改写 | 独立事务立即提交，不占用搬运批次额度；正文随块一起搬到目标槽 |
| 崩溃恢复 | 每批是一个 SQLite 事务（rollback journal + `synchronous=FULL`）；提交前 `os._exit(77)`，重启后 journal 自动回滚，已提交批次保留 |

### 搬运算法：位置置换的环分解 + 单临时槽

换条带本质是 32 个盘位之间的一个置换。规划器**按当前盘位**（不假设永远是初始布局，
因此可连续再次换条带）把置换分解成若干环，每个非平凡环：

1. `stage`：环起点的块写入临时槽，起点腾出“洞”；
2. 沿环反向逐条 `move`：把块写进洞，洞移动到该块原来的槽；
3. `unstage`：临时槽里的块写进最后一个洞。

每个环只暂存一块，所有操作都计入批次写入次数。

## 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

SQLite 数据文件放在命名卷 `volume-data`（容器内 `/data/volume.db`），容器重启后
已提交状态保留。

### 本地 Python

```bash
pip install -r requirements.txt
python -m flask --app app.server run --host 0.0.0.0 --port 8000 \
    --without-threads --no-reload --no-debugger
```

数据库路径由环境变量 `VIRTUAL_VOLUME_DB` 覆盖，默认 `data/volume.db`。
注意必须带 `--no-reload`，否则模拟崩溃时 reloader 监控进程会重新拉起服务。

## 网页操作

- **开始换条带**：选择每 2 块或每 4 块换盘，生成搬运计划。
- **执行一批并提交**：填写本批最大写入次数（含临时槽）；勾选“写槽后、提交前退出”
  会让进程以退出码 77 退出，页面自动轮询等待重启后刷新。
- **在线读 / 改写**：输入 LBN（0..31）；改写正文不足 4 KiB 自动补零、超长截断，
  立即提交。块在临时槽时同样可读写。
- 页面展示：迁移状态卡（进度、本批写入次数、临时槽占用、校验结果）、两块盘 16 槽
  位图（含每槽目标 LBN、空洞、暂存标记）、32 个逻辑块的当前盘位/目标盘位明细表、
  最近 25 条事件日志。

## HTTP API（测试/脚本用）

| 方法与路径 | 说明 |
| --- | --- |
| `GET /api/snapshot` | 全量状态（块位置、临时槽、迁移进度、事件、校验） |
| `POST /api/migration/start` `{"stripe_k":2\|4}` | 开始换条带 |
| `POST /api/migration/batch` `{"budget":N,"crash":bool}` | 执行一批；`crash=true` 时提交前退出 |
| `GET /api/blocks/<lbn>` | 按 LBN 读（自动识别临时槽），返回 hex 正文 |
| `PUT /api/blocks/<lbn>` `{"body_hex":...}` 或 `{"body":"..."}` | 按 LBN 改写，必须正好 4096 字节 |
| `GET /api/verify` | 校验：32 槽齐全、正文 4 KiB、临时槽 ≤1、完成后盘位符合新条带 |

## 测试

```bash
python tests/e2e_test.py
```

测试自动起停 Flask 子进程，覆盖：规划器环分解正确性（含从 k=2 布局再次规划 k=4）、
初始布局、额度 1 推进、临时槽块与盘上块的在线改写、两次“提交前崩溃 + 重启回滚”、
已提交改写在重启后保留、k=2/k=4 搬完后的逐块盘位核对。当前结果：**219 项检查全部通过**。

## 目录

```
app/db.py          SQLite 连接、建表、初始装满
app/migration.py   条带数学、环分解规划、批次执行引擎、在线 I/O、校验
app/server.py      Flask 路由（网页 + JSON API）
app/templates/     操作页模板
tests/e2e_test.py  端到端测试
Dockerfile, docker-compose.yaml, requirements.txt
```

## 已知限制 / 未做项

- 未在本机实际执行 `docker compose build`（当前环境没有 docker 二进制），仅校验了
  compose 文件结构；镜像基于官方 `python:3.11-slim`，命令与本地验证一致。
- 使用 Flask 内置开发服务器（单线程），未上 gunicorn/uvicorn 等生产 WSGI。
- 正文为模拟数据（初始块头 + 零填充），未模拟真实磁盘延迟、校验和或冗余副本。
