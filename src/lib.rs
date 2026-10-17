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

//! Standalone PostgreSQL → Arrow → DataFusion connector prototype.

mod plan;
mod postgres;
mod sql;

use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext};

pub use crate::postgres::{PgTable, ScanMetrics, ScanRecord};

/// The visible synthetic time index required by the proposed GreptimeDB design.
pub const TIME_COLUMN: &str = "greptime_timestamp";

/// A query joining remote customer dimensions with local Arrow metrics.
pub const JOIN_SQL: &str = "SELECT c.level, COUNT(*) AS requests, \
    AVG(m.latency) AS avg_latency \
    FROM metrics m JOIN customers c ON m.customer_id = c.id \
    WHERE c.id >= 2 AND c.active = true \
    GROUP BY c.level ORDER BY c.level";

/// Registers the remote provider and small local metrics for the demonstration.
/// The local table is a MemTable; it does not pretend to be a GreptimeDB region.
pub fn demo_context(customers: Arc<PgTable>) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_table("customers", customers)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("latency", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3, 5, 5, 999])),
            Arc::new(Float64Array::from(vec![
                10.0, 30.0, 20.0, 100.0, 200.0, 300.0, 500.0,
            ])),
        ],
    )?;
    ctx.register_table(
        "metrics",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    Ok(ctx)
}
