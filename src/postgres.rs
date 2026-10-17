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

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, RecordBatch, RecordBatchOptions, StringBuilder,
    TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgRow};
use sqlx::{ConnectOptions, PgConnection, PgPool, Postgres, Row};
use tokio::time::{Instant, timeout_at};

use crate::TIME_COLUMN;
use crate::plan::PgScanExec;
use crate::sql::{Parameter, quote_identifier, translate};

/// Execution evidence contains placeholders and parameter counts, never DSNs/values.
#[derive(Debug, Clone)]
pub struct ScanRecord {
    pub sql: String,
    pub parameter_count: usize,
}

/// Counts batches emitted by the source, before local filters or aggregation.
#[derive(Debug, Clone, Copy)]
pub struct ScanMetrics {
    pub rows: usize,
    pub batches: usize,
}

#[derive(Default)]
struct Trace {
    scans: Mutex<Vec<ScanRecord>>,
    rows: AtomicUsize,
    batches: AtomicUsize,
}

/// Read-only provider with a frozen, metadata-derived schema and one shared pool.
#[derive(Clone)]
pub struct PgTable {
    pool: PgPool,
    relation: String,
    schema: SchemaRef,
    batch_size: usize,
    trace: Arc<Trace>,
}

impl fmt::Debug for PgTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgTable")
            .field("relation", &self.relation)
            .field("schema", &self.schema)
            .field("batch_size", &self.batch_size)
            .finish_non_exhaustive()
    }
}

impl PgTable {
    /// Opens a bounded pool and inspects pg_catalog, including for an empty table.
    /// The DSN and raw driver errors are deliberately excluded from diagnostics.
    pub async fn connect(dsn: &str, schema: &str, table: &str, batch_size: usize) -> Result<Self> {
        if batch_size == 0 || batch_size > 65536 {
            return Err(execution_error("batch_size must be between 1 and 65536"));
        }
        for name in [schema, table] {
            if name.is_empty() || name.contains('\0') || name.len() > 63 {
                return Err(execution_error(
                    "PostgreSQL identifiers must contain 1..63 bytes and no NUL",
                ));
            }
        }
        let options = dsn
            .parse::<PgConnectOptions>()
            .map_err(|_| execution_error("invalid PostgreSQL DSN (details hidden)"))?
            .application_name("ospp_postgres_connector_demo")
            .options([
                ("default_transaction_read_only", "on"),
                ("statement_timeout", "3000"),
                // PG14+ on Linux otherwise may only notice a disconnected
                // client after a lock wait finishes. This is session-local.
                ("client_connection_check_interval", "100"),
                ("timezone", "UTC"),
            ])
            .disable_statement_logging();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(options)
            .await
            .map_err(|e| database_error("connect", &e))?;

        // Only catalog metadata is collected; table data always uses fetch().
        let columns = sqlx::query(
            "SELECT a.attname AS name, t.typname AS type_name, tn.nspname AS type_schema, \
             t.typtype::text AS type_kind, a.attnotnull AS not_null \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
             JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace \
             WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p') \
             AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(&pool)
        .await
        .map_err(|e| database_error("schema discovery", &e))?;
        if columns.is_empty() {
            return Err(execution_error(
                "table not found, not an ordinary table, or has no columns",
            ));
        }
        let mut fields = Vec::with_capacity(columns.len() + 1);
        for column in columns {
            let name: String = column
                .try_get("name")
                .map_err(|e| database_error("metadata decode", &e))?;
            let type_name: String = column
                .try_get("type_name")
                .map_err(|e| database_error("metadata decode", &e))?;
            let type_schema: String = column
                .try_get("type_schema")
                .map_err(|e| database_error("metadata decode", &e))?;
            let type_kind: String = column
                .try_get("type_kind")
                .map_err(|e| database_error("metadata decode", &e))?;
            let not_null: bool = column
                .try_get("not_null")
                .map_err(|e| database_error("metadata decode", &e))?;
            if name == TIME_COLUMN {
                return Err(execution_error(
                    "remote column conflicts with greptime_timestamp",
                ));
            }
            if type_schema != "pg_catalog" || type_kind != "b" {
                return Err(execution_error(
                    "custom PostgreSQL types are unsupported in this prototype",
                ));
            }
            let data_type = match type_name.as_str() {
                "bool" => DataType::Boolean,
                "int2" => DataType::Int16,
                "int4" => DataType::Int32,
                "int8" => DataType::Int64,
                "float4" => DataType::Float32,
                "float8" => DataType::Float64,
                "text" | "varchar" => DataType::Utf8,
                "bytea" => DataType::Binary,
                _ => {
                    return Err(execution_error(&format!(
                        "unsupported PostgreSQL type {type_name:?} for column {name:?}"
                    )));
                }
            };
            fields.push(Field::new(name, data_type, !not_null));
        }
        fields.push(Field::new(
            TIME_COLUMN,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ));
        Ok(Self {
            pool,
            relation: format!("{}.{}", quote_identifier(schema), quote_identifier(table)),
            schema: Arc::new(Schema::new(fields)),
            batch_size,
            trace: Arc::default(),
        })
    }

    /// Returns executed scan requests; building an EXPLAIN plan does not add one.
    pub fn executed_scans(&self) -> Result<Vec<ScanRecord>> {
        Ok(self
            .trace
            .scans
            .lock()
            .map_err(|_| execution_error("scan trace lock poisoned"))?
            .clone())
    }

    /// Returns source counters for proving that rows were emitted in small batches.
    pub fn metrics(&self) -> ScanMetrics {
        ScanMetrics {
            rows: self.trace.rows.load(Ordering::Relaxed),
            batches: self.trace.batches.load(Ordering::Relaxed),
        }
    }

    fn record_scan(&self, spec: &ScanSpec) -> Result<()> {
        self.trace
            .scans
            .lock()
            .map_err(|_| execution_error("scan trace lock poisoned"))?
            .push(ScanRecord {
                sql: spec.sql.clone(),
                parameter_count: spec.parameters.len(),
            });
        Ok(())
    }

    /// Releases this provider's pool once all users of its scans have finished.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    pub(crate) fn scan_spec(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<ScanSpec> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let schema = Arc::new(self.schema.project(&projection)?);
        let remote_columns = schema
            .fields()
            .iter()
            .filter(|field| field.name() != TIME_COLUMN)
            .map(|field| quote_identifier(field.name()))
            .collect::<Vec<_>>();
        // SELECT 1 preserves cardinality for COUNT(*) or synthetic-only scans.
        let select = if remote_columns.is_empty() {
            "1".to_owned()
        } else {
            remote_columns.join(", ")
        };
        let mut sql = format!("SELECT {select} FROM {}", self.relation);
        let mut parameters = Vec::new();
        let mut predicates = Vec::new();
        for filter in filters {
            let predicate = translate(filter, &self.schema, parameters.len())
                .ok_or_else(|| execution_error("scan received a filter that is not Exact"))?;
            predicates.push(predicate.sql);
            parameters.extend(predicate.parameters);
        }
        if !predicates.is_empty() {
            sql.push_str(&format!(" WHERE {}", predicates.join(" AND ")));
        }
        // DataFusion supplies this scan limit only when residual filters allow
        // it. Tests cover string residual + LIMIT. Unsupported filters supplied
        // directly above fail, rather than being dropped before this limit.
        if let Some(limit) = limit {
            let limit =
                i64::try_from(limit).map_err(|_| execution_error("scan LIMIT exceeds i64"))?;
            parameters.push(Parameter::Int64(limit));
            sql.push_str(&format!(" LIMIT ${}", parameters.len()));
        }
        Ok(ScanSpec {
            sql,
            parameters,
            schema,
        })
    }

    pub(crate) fn stream(
        &self,
        spec: ScanSpec,
    ) -> datafusion::execution::SendableRecordBatchStream {
        let table = self.clone();
        let schema = spec.schema.clone();
        let stream = async_stream::try_stream! {
            let deadline = Instant::now() + Duration::from_secs(5);
            let connection = timeout_at(deadline, table.pool.acquire()).await
                .map_err(|_| execution_error("PostgreSQL scan timed out acquiring a connection"))?
                .map_err(|e| database_error("acquire", &e))?;
            let mut lease = ScanLease(Some(connection));
            table.record_scan(&spec)?;
            let mut query = sqlx::query(&spec.sql);
            for parameter in &spec.parameters {
                query = match parameter {
                    Parameter::Int16(value) => query.bind(*value),
                    Parameter::Int32(value) => query.bind(*value),
                    Parameter::Int64(value) => query.bind(*value),
                    Parameter::Boolean(value) => query.bind(*value),
                };
            }
            let mut rows = query.fetch(lease.connection()?);
            loop {
                let mut builders = spec.schema.fields().iter().map(|field| ColumnBuilder::new(field))
                    .collect::<Result<Vec<_>>>()?;
                let mut count = 0;
                while count < table.batch_size {
                    let row = timeout_at(deadline, rows.try_next()).await
                        .map_err(|_| execution_error("PostgreSQL scan exceeded its five-second deadline"))?
                        .map_err(|e| database_error("scan", &e))?;
                    let Some(row) = row else { break };
                    let mut remote_index = 0;
                    for builder in &mut builders {
                        builder.append(&row, &mut remote_index)?;
                    }
                    count += 1;
                }
                if count == 0 { break; }
                let arrays = builders.iter_mut().map(ColumnBuilder::finish).collect::<Vec<_>>();
                let options = RecordBatchOptions::new().with_row_count(Some(count));
                let batch = RecordBatch::try_new_with_options(spec.schema.clone(), arrays, &options)?;
                table.trace.rows.fetch_add(count, Ordering::Relaxed);
                table.trace.batches.fetch_add(1, Ordering::Relaxed);
                yield batch;
                if count < table.batch_size { break; }
            }
            drop(rows);
            // Only a completely consumed, successfully decoded scan returns a
            // healthy connection. Drop/error/timeout closes a partial result.
            lease.release();
        };
        Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream))
    }
}

#[async_trait::async_trait]
impl TableProvider for PgTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let spec = self.scan_spec(projection, filters, limit)?;
        Ok(Arc::new(PgScanExec::new(self.clone(), spec)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if translate(filter, &self.schema, 0).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ScanSpec {
    pub sql: String,
    pub parameters: Vec<Parameter>,
    pub schema: SchemaRef,
}

/// Owns the connection across yields. Early stream drop closes it asynchronously.
struct ScanLease(Option<PoolConnection<Postgres>>);

impl ScanLease {
    fn connection(&mut self) -> Result<&mut PgConnection> {
        self.0
            .as_deref_mut()
            .ok_or_else(|| execution_error("scan lease already released"))
    }

    fn release(&mut self) {
        drop(self.0.take());
    }
}

impl Drop for ScanLease {
    fn drop(&mut self) {
        if let Some(mut connection) = self.0.take() {
            connection.close_on_drop();
        }
    }
}

enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    Binary(BinaryBuilder),
    Timestamp(TimestampMillisecondBuilder),
}

impl ColumnBuilder {
    fn new(field: &Field) -> Result<Self> {
        Ok(match field.data_type() {
            DataType::Boolean => Self::Boolean(BooleanBuilder::new()),
            DataType::Int16 => Self::Int16(Int16Builder::new()),
            DataType::Int32 => Self::Int32(Int32Builder::new()),
            DataType::Int64 => Self::Int64(Int64Builder::new()),
            DataType::Float32 => Self::Float32(Float32Builder::new()),
            DataType::Float64 => Self::Float64(Float64Builder::new()),
            DataType::Utf8 => Self::String(StringBuilder::new()),
            DataType::Binary => Self::Binary(BinaryBuilder::new()),
            DataType::Timestamp(TimeUnit::Millisecond, None) if field.name() == TIME_COLUMN => {
                Self::Timestamp(TimestampMillisecondBuilder::new())
            }
            _ => return Err(execution_error("unexpected Arrow type in frozen schema")),
        })
    }

    fn append(&mut self, row: &PgRow, remote_index: &mut usize) -> Result<()> {
        let index = *remote_index;
        let result = match self {
            Self::Boolean(builder) => row
                .try_get::<Option<bool>, _>(index)
                .map(|v| builder.append_option(v)),
            Self::Int16(builder) => row
                .try_get::<Option<i16>, _>(index)
                .map(|v| builder.append_option(v)),
            Self::Int32(builder) => row
                .try_get::<Option<i32>, _>(index)
                .map(|v| builder.append_option(v)),
            Self::Int64(builder) => row
                .try_get::<Option<i64>, _>(index)
                .map(|v| builder.append_option(v)),
            Self::Float32(builder) => row
                .try_get::<Option<f32>, _>(index)
                .map(|v| builder.append_option(v)),
            Self::Float64(builder) => row
                .try_get::<Option<f64>, _>(index)
                .map(|v| builder.append_option(v)),
            Self::String(builder) => row
                .try_get::<Option<&str>, _>(index)
                .map(|v| builder.append_option(v)),
            Self::Binary(builder) => row
                .try_get::<Option<&[u8]>, _>(index)
                .map(|v| builder.append_option(v)),
            Self::Timestamp(builder) => {
                builder.append_value(0);
                return Ok(());
            }
        };
        result.map_err(|e| database_error("row decode; remote schema may have changed", &e))?;
        *remote_index += 1;
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::String(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::Timestamp(b) => Arc::new(b.finish()),
        }
    }
}

fn execution_error(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.to_owned())
}

fn database_error(stage: &str, error: &sqlx::Error) -> DataFusionError {
    // Do not attach the driver error as a source: backend messages can echo
    // credentials, SQL text, or attacker-controlled data. SQLSTATE is sufficient
    // for this prototype's failure diagnostics.
    let code = match error {
        sqlx::Error::Database(e) => e
            .code()
            .filter(|code| code.len() == 5 && code.bytes().all(|b| b.is_ascii_alphanumeric()))
            .map(|code| code.into_owned()),
        _ => None,
    };
    execution_error(&format!(
        "PostgreSQL {stage} failed (SQLSTATE {}; details hidden)",
        code.as_deref().unwrap_or("unavailable")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hides_raw_driver_errors() {
        let error = sqlx::Error::Protocol("password=do-not-print; postgres://secret".to_owned());
        let output = format!("{:?}", database_error("scan", &error));
        assert!(!output.contains("do-not-print"));
        assert!(!output.contains("postgres://"));
    }
}
