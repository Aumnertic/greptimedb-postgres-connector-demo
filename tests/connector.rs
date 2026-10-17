// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{
    Array, BinaryArray, Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use postgres_connector_demo::{JOIN_SQL, PgTable, TIME_COLUMN, demo_context};
use sqlx::{Executor, PgPool, Row};
use tokio::time::{Instant, sleep, timeout};

fn dsn() -> String {
    std::env::var("DEMO_DATABASE_URL").expect("set DEMO_DATABASE_URL; see README")
}

async fn provider(table: &str, batch_size: usize) -> Arc<PgTable> {
    Arc::new(
        PgTable::connect(&dsn(), "connector_demo", table, batch_size)
            .await
            .unwrap(),
    )
}

fn context(table: Arc<PgTable>) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_table("customers", table).unwrap();
    ctx
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

async fn admin_pool() -> PgPool {
    PgPool::connect(
        &std::env::var("DEMO_ADMIN_DATABASE_URL")
            .expect("set DEMO_ADMIN_DATABASE_URL for fixture locks"),
    )
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires the isolated PostgreSQL compose fixture"]
async fn maps_supported_types_nulls_and_integer_boundaries() {
    let table = provider("supported_types", 2).await;
    assert_eq!(table.schema().field(0).data_type(), &DataType::Int16);
    assert_eq!(table.schema().field(1).data_type(), &DataType::Int32);
    assert_eq!(table.schema().field(2).data_type(), &DataType::Int64);
    let batches = query(&context(table.clone()), "SELECT * FROM customers").await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let batch = &batches[0];
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        i16::MIN
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        i32::MIN
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MIN
    );
    assert_eq!(
        batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "你好 PostgreSQL"
    );
    assert_eq!(
        batch
            .column(7)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &[0, 255]
    );
    for column in &batch.columns()[..8] {
        assert!(column.is_null(1));
    }
    table.close().await;
}

#[tokio::test]
#[ignore = "requires the isolated PostgreSQL compose fixture"]
async fn infers_empty_schema_and_rejects_unsupported_types_and_collisions() {
    let empty = provider("empty_customers", 2).await;
    assert_eq!(empty.schema().fields().len(), 6);
    assert_eq!(empty.schema().field(0).data_type(), &DataType::Int64);
    let batches = query(&context(empty.clone()), "SELECT COUNT(*) FROM customers").await;
    assert_eq!(ids(&batches), vec![0]);
    for (name, expected) in [
        ("unsupported_type", "numeric"),
        ("timestamp_collision", "conflicts"),
    ] {
        let error = PgTable::connect(&dsn(), "connector_demo", name, 2)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains("demo_reader_password"));
    }
    empty.close().await;
}

#[tokio::test]
#[ignore = "requires the isolated PostgreSQL compose fixture"]
async fn pushes_projection_integer_predicate_and_bound_limit() {
    let table = provider("customers", 2).await;
    let batches = query(
        &context(table.clone()),
        "SELECT name FROM customers WHERE id >= 2 LIMIT 2",
    )
    .await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let scans = table.executed_scans().unwrap();
    assert_eq!(scans.len(), 1);
    assert!(
        scans[0].sql.starts_with("SELECT \"name\" FROM"),
        "{:?}",
        scans
    );
    assert!(scans[0].sql.contains("\"id\" >= $1"), "{:?}", scans);
    assert!(scans[0].sql.ends_with("LIMIT $2"), "{:?}", scans);
    assert_eq!(scans[0].parameter_count, 2);
    table.close().await;
}

#[tokio::test]
#[ignore = "requires the isolated PostgreSQL compose fixture"]
async fn residual_string_filter_prevents_early_limit_and_missing_rows() {
    let table = provider("customers", 2).await;
    let batches = query(
        &context(table.clone()),
        "SELECT id FROM customers WHERE level = 'gold' LIMIT 2",
    )
    .await;
    let mut result = ids(&batches);
    result.sort();
    assert_eq!(result, vec![3, 5]);
    let scans = table.executed_scans().unwrap();
    assert!(!scans[0].sql.contains("LIMIT"), "{:?}", scans);
    assert!(!scans[0].sql.contains("gold"));
    assert!(scans[0].sql.contains("\"level\""));
    table.close().await;
}

#[tokio::test]
#[ignore = "requires the isolated PostgreSQL compose fixture"]
async fn preserves_count_empty_projection_and_synthetic_timestamp() {
    let table = provider("customers", 2).await;
    let ctx = context(table.clone());
    assert_eq!(
        ids(&query(&ctx, "SELECT COUNT(*) FROM customers").await),
        vec![6]
    );
    assert!(
        table.executed_scans().unwrap()[0]
            .sql
            .starts_with("SELECT 1 FROM")
    );
    let batches = query(&ctx, "SELECT greptime_timestamp FROM customers").await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert!(values.values().iter().all(|value| *value == 0));
        assert_eq!(values.null_count(), 0);
    }
    let state = ctx.state();
    let plan = table.scan(&state, Some(&vec![]), &[], None).await.unwrap();
    let batches = plan
        .execute(0, ctx.task_ctx())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    assert!(
        ctx.sql(&format!(
            "SELECT id FROM customers WHERE {TIME_COLUMN} > TIMESTAMP '1970-01-01 00:00:00' LIMIT 1"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .all(|batch| batch.num_rows() == 0)
    );
    table.close().await;
}

#[tokio::test]
#[ignore = "requires the isolated PostgreSQL compose fixture"]
async fn local_join_aggregation_matches_direct_postgres_baseline() {
    let table = provider("customers", 2).await;
    let batches = query(&demo_context(table.clone()).unwrap(), JOIN_SQL).await;
    let mut actual = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            actual.push((
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .to_owned(),
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row),
            ));
        }
    }
    let pool = PgPool::connect(&dsn()).await.unwrap();
    let rows = sqlx::query(
        "WITH metrics(customer_id, latency) AS (VALUES \
         (1::bigint,10::float8),(1,30),(2,20),(3,100),(5,200),(5,300),(999,500)) \
         SELECT c.level, COUNT(*) AS requests, AVG(m.latency) AS avg_latency \
         FROM metrics m JOIN connector_demo.customers c ON m.customer_id=c.id \
         WHERE c.id >= 2 AND c.active = true GROUP BY c.level ORDER BY c.level",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let expected = rows
        .iter()
        .map(|row| {
            (
                row.get::<String, _>("level"),
                row.get::<i64, _>("requests"),
                row.get::<f64, _>("avg_latency"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_eq!(
        actual,
        vec![
            ("gold".to_owned(), 3, 200.0),
            ("silver".to_owned(), 1, 20.0)
        ]
    );
    assert!(
        table
            .executed_scans()
            .unwrap()
            .iter()
            .all(|scan| !scan.sql.contains("JOIN") && !scan.sql.contains("GROUP BY"))
    );
    pool.close().await;
    table.close().await;
}

#[tokio::test]
#[ignore = "requires the isolated PostgreSQL compose fixture"]
async fn quotes_remote_identifiers_and_keeps_injection_strings_local() {
    let odd = provider("odd\"table", 2).await;
    let result = query(&context(odd.clone()), "SELECT \"odd\"\"id\" FROM customers").await;
    assert_eq!(ids(&result), vec![7]);
    assert!(
        odd.executed_scans().unwrap()[0]
            .sql
            .contains("\"odd\"\"table\"")
    );
    let table = provider("customers", 2).await;
    let ctx = context(table.clone());
    let result = query(
        &ctx,
        "SELECT id FROM customers WHERE name = 'x'' OR true --'",
    )
    .await;
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    assert!(!table.executed_scans().unwrap()[0].sql.contains("OR true"));
    assert_eq!(
        ids(&query(&ctx, "SELECT COUNT(*) FROM customers").await),
        vec![6]
    );
    for sql in [
        "INSERT INTO customers (id, name, active) VALUES (999, 'bad', true)",
        "DELETE FROM customers WHERE id = 1",
        "UPDATE customers SET name = 'bad' WHERE id = 1",
    ] {
        let result = match ctx.sql(sql).await {
            Ok(frame) => frame.collect().await,
            Err(error) => Err(error),
        };
        assert!(result.is_err(), "write was accepted: {sql}");
    }
    let pool = PgPool::connect(&dsn()).await.unwrap();
    let error =
        sqlx::query("INSERT INTO connector_demo.customers(id,name,active) VALUES(999,'bad',true)")
            .execute(&pool)
            .await
            .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("42501")
    );
    assert!(!format!("{table:?}").contains("demo_reader_password"));
    pool.close().await;
    odd.close().await;
    table.close().await;
}

#[tokio::test]
#[ignore = "requires the isolated PostgreSQL compose fixture"]
async fn explain_is_lazy_and_plan_reexecutes_after_partial_drop() {
    let table = provider("customers", 2).await;
    let ctx = context(table.clone());
    let state = ctx.state();
    let plan = table.scan(&state, Some(&vec![0]), &[], None).await.unwrap();
    assert!(
        displayable(plan.as_ref())
            .indent(true)
            .to_string()
            .contains("PgScanExec")
    );
    assert!(table.executed_scans().unwrap().is_empty());
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    assert!(table.executed_scans().unwrap().is_empty());
    assert_eq!(stream.try_next().await.unwrap().unwrap().num_rows(), 2);
    drop(stream);
    for _ in 0..2 {
        let batches = timeout(
            Duration::from_secs(3),
            plan.execute(0, ctx.task_ctx())
                .unwrap()
                .try_collect::<Vec<_>>(),
        )
        .await
        .unwrap()
        .unwrap();
        let mut result = ids(&batches);
        result.sort();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
        assert!(batches.iter().all(|batch| batch.num_rows() <= 2));
    }
    assert_eq!(table.metrics().rows, 14);
    assert_eq!(table.metrics().batches, 7);
    table.close().await;
}

#[tokio::test]
#[ignore = "requires the isolated PostgreSQL compose fixture"]
async fn streams_ten_thousand_rows_with_bounded_batches_and_pull_backpressure() {
    let table = provider("stream_rows", 32).await;
    let ctx = context(table.clone());
    let plan = table.scan(&ctx.state(), None, &[], None).await.unwrap();
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    assert_eq!(stream.try_next().await.unwrap().unwrap().num_rows(), 32);
    sleep(Duration::from_millis(50)).await;
    assert_eq!(table.metrics().rows, 32);
    assert_eq!(table.metrics().batches, 1);
    let mut rows = 32;
    while let Some(batch) = stream.try_next().await.unwrap() {
        assert!(batch.num_rows() <= 32);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 10000);
    assert_eq!(table.metrics().rows, 10000);
    assert_eq!(table.metrics().batches, 313);
    table.close().await;
}

#[tokio::test]
#[ignore = "requires the isolated PostgreSQL compose fixture and owner URL"]
async fn server_timeout_closes_failed_scan_and_pool_recovers() {
    let table = provider("customers", 2).await;
    let ctx = context(table.clone());
    let owner = admin_pool().await;
    let mut transaction = owner.begin().await.unwrap();
    transaction
        .execute("LOCK TABLE connector_demo.customers IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let plan = table
        .scan(&ctx.state(), Some(&vec![0]), &[], None)
        .await
        .unwrap();
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    let start = Instant::now();
    let error = timeout(Duration::from_secs(5), stream.try_next())
        .await
        .unwrap()
        .unwrap_err()
        .to_string();
    assert!(error.contains("57014"), "{error}");
    assert!(start.elapsed() >= Duration::from_secs(2));
    assert!(!error.contains("demo_reader_password"));
    drop(stream);
    transaction.rollback().await.unwrap();
    assert_eq!(
        ids(&query(&ctx, "SELECT COUNT(*) FROM customers").await),
        vec![6]
    );
    owner.close().await;
    table.close().await;
}

#[tokio::test]
#[ignore = "requires the isolated PostgreSQL compose fixture and owner URL; run serially"]
async fn dropping_blocked_scan_clears_remote_activity_and_allows_another_query() {
    let table = provider("customers", 2).await;
    let ctx = context(table.clone());
    let owner = admin_pool().await;
    let mut transaction = owner.begin().await.unwrap();
    transaction
        .execute("LOCK TABLE connector_demo.customers IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let plan = table
        .scan(&ctx.state(), Some(&vec![0]), &[], None)
        .await
        .unwrap();
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    assert!(
        timeout(Duration::from_millis(150), stream.try_next())
            .await
            .is_err()
    );
    let blocked: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name='ospp_postgres_connector_demo' AND state='active' AND wait_event_type='Lock'").fetch_one(&owner).await.unwrap();
    assert_eq!(
        blocked, 1,
        "the scan must actually reach PostgreSQL before cancellation"
    );
    drop(stream);
    timeout(Duration::from_secs(2), async {
        loop {
            let active: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name='ospp_postgres_connector_demo' AND state='active' AND wait_event_type='Lock'").fetch_one(&owner).await.unwrap();
            if active == 0 { break; }
            sleep(Duration::from_millis(25)).await;
        }
    }).await.unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(
        ids(&query(&ctx, "SELECT COUNT(*) FROM customers").await),
        vec![6]
    );
    owner.close().await;
    table.close().await;
}
