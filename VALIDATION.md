# Demo 实际验证记录

2026-10-17，在 SSH 主机 `dev`（`4090pc`，Linux x86_64）完成。
演示源码位于 `/home/zed/projects/greptimedb-ospp/demo/postgres-connector`，
GreptimeDB checkout 基线为 `28ec3ff5aad8d274d83ae42f96dc54f838fb6b20`。
本轮只新增独立原型，没有修改 GreptimeDB 的产品源码和根 Cargo.lock。

| 验证 | 实际结果 |
| --- | --- |
| Rust 构建 | `cargo build --locked --jobs 2`，退出码 0 |
| Nextest | **15 通过、0 失败、0 跳过**，执行耗时 4.295s |
| Clippy | `--all-targets -- -D warnings`，退出码 0 |
| 格式检查 | `cargo fmt --all -- --check`，退出码 0 |
| 默认六条演示 SQL | 全部执行成功，退出码 0，见[原始输出](demo-output.txt) |
| 独立 EXPLAIN | 计划含远端占位符 SQL，源端读取 **0 行、0 batch** |
| JOIN / 聚合差分 | 与 PostgreSQL 中相同指标 VALUES + 真实 customers 表的结果完全相同 |
| 10,000 行扫描 | batch_size=32，实际产出 313 个 batch；暂停消费时不继续生成 Arrow batch |
| 提前丢弃扫描流 | 后续查询可重新借用连接；同一物理计划也可再次完整执行 |
| 取消等待锁的扫描 | 先确认远端确实正在等待锁，丢弃流后在 2s 检查时限内消失，后续查询成功 |
| 远端超时 | 等待锁时约 3s 返回 SQLSTATE `57014`，后续查询成功 |

本次 Nextest run ID：`70bc02ff-2b74-4a76-a750-ec69c071ed76`。
其中 4 项为纯翻译/错误脱敏单测，11 项使用真实 PostgreSQL；后者在默认运行时
标记 ignored，本次通过 `--run-ignored all` 全部启用。加锁用例串行运行。

## 环境和重现入口

- Rust `1.98.1`、SQLx `0.8.6`、Arrow `59.2.0`、GreptimeTeam DataFusion 固定 fork。
- 容器镜像 `postgres:14-alpine`，本次实际报告 PostgreSQL `14.24`。
- 独立 Compose project：`ospp-pg-connector-demo`，仅监听 `127.0.0.1:55432`。
- 使用只拥有 schema USAGE 和表 SELECT 权限的 `connector_reader`；owner 账号仅初始化/加锁测试使用。
- 网络下载统一使用 `http://172.22.236.100:7897`；本地数据库地址列入 `NO_PROXY`。
- 编译沿用上一轮远端验证设置：CPU `16–23`、两条编译任务、mold 链接器、
  `RUST_MIN_STACK=33554432`、无调试信息、禁用增量编译。
- 复用 `/home/zed/projects/greptimedb-ospp/target` 的构建缓存。

完整命令、环境变量和演示步骤见 [README](README.md)。
已构建好的程序可直接运行，避免现场再次编译：

```bash
ssh dev 'cd /home/zed/projects/greptimedb-ospp/demo/postgres-connector && bash run.sh --prebuilt'
```

## 可向导师展示的两个关键结果

整数过滤可以精确执行，投影、条件和 LIMIT 都下推，值为参数：

```sql
SELECT "id", "name" FROM "connector_demo"."customers"
WHERE ("id" >= $1) LIMIT $2
```

`WHERE level = 'gold' LIMIT 2` 的字符串过滤仍在本地。远端实际 SQL 不带 LIMIT，
读取六行、生成三个 batch，最终准确返回客户 3 和 5：

```sql
SELECT "id", "name", "level" FROM "connector_demo"."customers"
```

联合查询的计划包含本地 `HashJoinExec`、`AggregateExec`，远端仅做投影和过滤；
结果为 `gold → 3 / 200.0`、`silver → 1 / 20.0`（请求数 / 平均延迟）。

## 取消试验中的发现

第一轮真实数据库测试发现：SQLx `close_on_drop()` 虽然关闭连接，但服务端
等待锁的查询未在两秒内消失。这个失败保留在 `nextest-initial.log`。
原型随后仅为自己的 PostgreSQL 连接设置
`client_connection_check_interval=100ms`，保留 `statement_timeout=3s`，
修正后的测试先确认实际等待锁，再验证丢弃流后远端活动消失。

断连检查是当前 Linux / PostgreSQL 14+ 原型的前提，其他平台和分布式取消
仍需独立设计，不能把本轮结果泛化为任意 SQLx 查询均可即时取消。
参数行为见 [PostgreSQL 官方文档](https://www.postgresql.org/docs/14/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL)。

## 原始日志

远端目录：`/home/zed/projects/greptimedb-ospp-runs/20261017-connector-demo/`。
保存了 `nextest.log`、`clippy.log`、`build-final.log`、`demo-output.log`、
`explain-output.log`、各步骤退出码和首次取消失败记录。
本仓库的 [validation/](validation/) 收录了最终测试、Clippy、构建、EXPLAIN 和首次取消失败的原始日志。
可分享的默认演示原始输出另外保存在 [demo-output.txt](demo-output.txt)。

演示容器和数据 volume 保留，方便继续运行；这里只验证独立 connector + DataFusion。此独立仓库从上述已验证的 demo 目录整理而来，Rust 源码和 Cargo.lock 保持一致。
尚未实现 GreptimeDB DDL、catalog、Region Engine、恢复和分布式执行，也未实现 MySQL。
