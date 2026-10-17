# PostgreSQL connector 演示原型

这是申请开源之夏、与导师讨论方案用的独立 Rust 程序。实际链路为：

```text
PostgreSQL 业务表 → SQLx 行流 → Arrow RecordBatch → DataFusion TableProvider
                                                           ↓
本地 Arrow metrics 表 ────────────────────────────────── JOIN / 聚合
```

DataFusion 使用当前 GreptimeDB 相同的 fork/revision
`e3221b8f74b15c7192621576e5d1e354a6cf097a`，Arrow 为其依赖的 `59.2.0`，
SQLx 固定为 `0.8.6`。本目录有独立 Cargo workspace，只编译此原型及其依赖。

已在 `dev` 完成构建、15 项测试、Clippy 和六条演示 SQL，见
[实际验证记录](VALIDATION.md)和[演示原始输出](demo-output.txt)。

## 能演示什么

- 从 `pg_catalog` 推断列类型和 NULL 属性，包括空表；schema 在注册后固定。
- 自动附加 `greptime_timestamp: Timestamp(Millisecond)`，值为 Unix epoch 0；重名时报错。
- Projection 下推，整数/布尔比较、NULL 判断和安全的 AND/OR/NOT 下推。
- 所有下推值及 LIMIT 使用 `$1` 等参数绑定；schema、表、列名分别引用。
- 字符串比较、浮点比较及合成列条件交给本地 DataFusion。残余过滤存在时不提前下推 LIMIT。
- 默认每批两行，直接从 SQLx `fetch()` 行流填充 Arrow builders。
- `COUNT(*)`、零列 projection、只投影合成列时，用远端 `SELECT 1` 保留行数。
- EXPLAIN 展示远端 SQL 与参数个数；执行前不发起远端扫描，每次执行创建新的流。
- 用远端客户维度关联本地指标，JOIN、聚合、排序由 DataFusion 执行。
- 连接池最多一条连接；正常读完可复用，提前停止、解码失败和超时关闭扫描连接。
- PostgreSQL `statement_timeout=3s`，客户端整体扫描期限 5s；当前 Linux/PG14+
  演示连接还设 `client_connection_check_interval=100ms`，取消试验检查 `pg_stat_activity`。
- DSN 不输出；错误仅保留操作阶段和 SQLSTATE，隐藏原始驱动错误。

支持的远端列类型：`SMALLINT`、`INTEGER`、`BIGINT`、`BOOLEAN`、`REAL`、
`DOUBLE PRECISION`、`TEXT`、`VARCHAR`、`BYTEA`。未知类型直接拒绝。
真实时间类型、Decimal、JSON、数组等还没有实现。

## 快速运行

需要 Rust `1.98.1`、C/C++ 编译工具与 Docker Compose。下面的命令在本目录执行。
所有账号密码都是本演示的公开测试数据，不连接真实业务库。

```bash
gh repo clone Aumnertic/greptimedb-postgres-connector-demo
cd greptimedb-postgres-connector-demo
docker compose -p ospp-pg-connector-demo up -d --wait

export DEMO_DATABASE_URL='postgres://connector_reader:demo_reader_password@127.0.0.1:55432/connector_demo'
cargo run --locked
```

Compose 创建独立容器与 volume，端口只绑定 `127.0.0.1:55432`，不会占用已有
5432 端口。`fixtures.sql` 只在新 volume 的首次初始化执行。运行账号只有
`connector_demo` schema 的读取权限；owner 账号只用于初始化和测试中的加锁。

默认运行六条查询，依次展示：参数化过滤及 NULL、合法的 LIMIT 下推、
字符串残余过滤与 LIMIT、JOIN 聚合、COUNT 的零列扫描、合成时间列。
每条查询显示物理计划、结果、源端行数/批次数和实际执行的远端 SQL。

例如，联合查询的预期结果为：

| level | requests | avg_latency |
| --- | ---: | ---: |
| gold | 3 | 200.0 |
| silver | 1 | 20.0 |

测试会用 PostgreSQL 的 `VALUES` 指标表和真实客户表再算一次，逐项比较结果。
本地 `metrics` 是 Arrow MemTable，演示时应向导师明确这一点。

可以按下面的顺序讲解，约三到五分钟：

1. 先说明范围：真实 PostgreSQL + 独立 DataFusion，验证 connector 关键路径；
   GreptimeDB catalog 和 Region Engine 是下一阶段。
2. 看第一条查询：远端只取需要的列，`id >= 3` 变成 `$1`，NULL 保留为空值。
3. 对比第二、三条查询：整数过滤可随 LIMIT 下推；字符串过滤留在本地，
   远端 SQL 没有 LIMIT。两个 gold 客户是 3 和 5，提前只读两行会漏掉它们。
4. 看 JOIN 计划和结果：远端 SQL 只有 SELECT，HashJoin 与 Aggregate 在本地；
   gold 的三条请求平均延迟为 200，silver 为 20。
5. 用 COUNT 和合成列说明为什么需要 `SELECT 1` 保留远端行数。
6. 展示验证记录，说明取消试验发现的驱动/服务端差异，再讨论怎样接入现有 scanner、
   固定 schema、region 生命周期与分布式取消链路。

也可以执行自己的 SELECT：

```bash
cargo run --locked -- --sql "SELECT id, name FROM customers WHERE level = 'gold' LIMIT 2"
cargo run --locked -- --sql "EXPLAIN SELECT name FROM customers WHERE id >= 2 LIMIT 2"
```

第二种查询只生成计划，源端扫描计数保持 0。不要把真实 DSN 放进 SQL、
命令行参数或演示截图；连接信息通过环境变量传入。

## 在 dev 上重现

源码位于 `/home/zed/projects/greptimedb-ospp/demo/postgres-connector`。
沿用上一轮验证的 CPU 亲和性和链接器设置，所有网络下载统一使用指定代理。

```bash
ssh dev
cd /home/zed/projects/greptimedb-ospp/demo/postgres-connector

export http_proxy=http://172.22.236.100:7897
export https_proxy="$http_proxy" HTTP_PROXY="$http_proxy" HTTPS_PROXY="$http_proxy"
export all_proxy="$http_proxy" ALL_PROXY="$http_proxy"
export no_proxy=localhost,127.0.0.1,::1 NO_PROXY=localhost,127.0.0.1,::1
export RUST_MIN_STACK=33554432 CARGO_INCREMENTAL=0
export RUSTFLAGS='-C link-arg=-fuse-ld=mold'
export CARGO_TARGET_DIR=/home/zed/projects/greptimedb-ospp/target
ulimit -n 65536

sudo -n docker compose -p ospp-pg-connector-demo up -d --wait
export DEMO_DATABASE_URL='postgres://connector_reader:demo_reader_password@127.0.0.1:55432/connector_demo'
taskset -c 16-23 cargo run --locked --jobs 2
```

已有编译结果时，可直接执行，适合导师沟通时现场演示：

```bash
bash run.sh --prebuilt
```

`run.sh` 默认使用公开的只读演示账号，也可通过 `DEMO_DATABASE_URL` 覆盖；
`--prebuilt` 复用已编译的二进制，不触发构建。独立仓库默认读取 `target/debug/`，
设置了 `CARGO_TARGET_DIR` 时读取对应目录，也可用 `DEMO_BINARY` 指定二进制。

## 验证

纯翻译/脱敏单测不需要 PostgreSQL。真实数据库用例标为 ignored，避免缺少演示库
时误运行；验证时显式启用全部用例并串行执行，因为取消/超时用例会对演示表加锁。

```bash
export DEMO_ADMIN_DATABASE_URL='postgres://demo_owner:demo_owner_password@127.0.0.1:55432/connector_demo'
cargo nextest run --locked --build-jobs 2 --test-threads 1 --run-ignored all
cargo fmt --all -- --check
cargo clippy --locked --all-targets --jobs 2 -- -D warnings
```

覆盖类型边界及 NULL、空表推断、不支持类型、合成列冲突、参数化过滤和 LIMIT、
残余过滤不丢行、零列扫描、JOIN 基准、恶意字符串/标识符、写入拒绝、
惰性 EXPLAIN、计划重复执行、提前丢弃流、10,000 行分批读取和拉取背压、
远端超时后恢复，以及取消后远端等待查询清理。

## 演示边界与后续接入

这份原型尚未接入 GreptimeDB 的 `CREATE EXTERNAL TABLE`、catalog、
Region Engine、datanode 路由、持久化或重启恢复，也不包含 MySQL connector。
GreptimeDB 原生表的联合查询需要后续集成；当前的本地指标表仅用于证明执行链路。

本实现按行数限制批次，没有单行/字节预算，不能据此宣称任意大字段下的内存上限。
SQLx/网络层也会自行缓冲；背压试验验证的是停止拉取时不继续构造 Arrow batch。
表结构在建表与扫描之间变化时，当前只保证明显的类型不匹配报错，尚无完整
schema 指纹复核。当前仅接受普通表和分区表，不开放任意 SQL 或 view 作为远端源。
取消用例覆盖当前 PostgreSQL/SQLx 组合，不代表已证明跨 region 的查询取消。
首次试验发现，单纯断开连接不能保证等待锁的查询及时消失；这里使用 Linux 下的
会话级断连检查补足这一点，其他平台需要独立取消策略，参见
[PostgreSQL 官方参数说明](https://www.postgresql.org/docs/14/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL)。
合成时间列也没有经过 GreptimeDB TIME INDEX 与存储元数据的集成验证。

接入时可以迁移 `sql.rs` 的无网络谓词分析和 PostgreSQL schema/解码逻辑，
用 relational Region Engine 的 scanner 替换 `PgScanExec`，并沿既有
`ScanRequest`、catalog 和 DDL procedure 补齐生命周期与分布式路径。

演示结束可停止自己的服务并保留数据：

```bash
docker compose -p ospp-pg-connector-demo stop
```
