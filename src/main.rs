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

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{displayable, execute_stream};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use postgres_connector_demo::{JOIN_SQL, PgTable, demo_context};

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args == ["--help"] {
        println!(
            "postgres-connector-demo [--sql 'SELECT ...']\nSet DEMO_DATABASE_URL to a PostgreSQL read-only connection. No DSN is printed."
        );
        return Ok(());
    }
    if !args.is_empty() && (args.len() != 2 || args[0] != "--sql") {
        return Err(DataFusionError::Execution(
            "usage: postgres-connector-demo [--sql 'SELECT ...']".to_owned(),
        ));
    }
    let dsn = std::env::var("DEMO_DATABASE_URL").map_err(|_| {
        DataFusionError::Execution("set DEMO_DATABASE_URL (see README.md)".to_owned())
    })?;
    let table = Arc::new(PgTable::connect(&dsn, "connector_demo", "customers", 2).await?);
    let ctx = demo_context(table.clone())?;
    println!("PostgreSQL connector prototype; standalone DataFusion, batch_size=2");
    println!("Schema discovered from pg_catalog (synthetic timestamp appended):");
    for field in table.schema().fields() {
        println!(
            "  {}: {:?}, nullable={}",
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }
    let queries = if args.is_empty() {
        vec![
            "SELECT id, name, credit FROM customers WHERE id >= 3 ORDER BY id",
            "SELECT id, name FROM customers WHERE id >= 2 LIMIT 2",
            "SELECT id, name FROM customers WHERE level = 'gold' LIMIT 2",
            JOIN_SQL,
            "SELECT COUNT(*) AS total FROM customers",
            "SELECT id, greptime_timestamp FROM customers WHERE id = 3",
        ]
    } else {
        vec![args[1].as_str()]
    };
    for sql in queries {
        run_query(&ctx, &table, sql).await?;
    }
    table.close().await;
    Ok(())
}

async fn run_query(ctx: &SessionContext, table: &PgTable, sql: &str) -> Result<()> {
    println!("\nSQL> {sql}");
    let before = table.metrics();
    let prior_scans = table.executed_scans()?.len();
    let frame = ctx.sql(sql).await?;
    let plan = frame.create_physical_plan().await?;
    println!("{}", displayable(plan.as_ref()).indent(true));
    let mut stream = execute_stream(plan, ctx.task_ctx())?;
    let mut rows = 0;
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows();
        if batch.num_rows() > 0 {
            println!("{}", pretty_format_batches(&[batch])?);
        }
    }
    let after = table.metrics();
    println!(
        "Output: {rows} rows; connector: {} rows in {} batches",
        after.rows - before.rows,
        after.batches - before.batches
    );
    for scan in table.executed_scans()?.iter().skip(prior_scans) {
        println!(
            "Remote SQL: {} [bound parameters: {}]",
            scan.sql, scan.parameter_count
        );
    }
    Ok(())
}
