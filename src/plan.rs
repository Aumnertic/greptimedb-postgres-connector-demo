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
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};

use crate::PgTable;
use crate::postgres::ScanSpec;

/// A repeatable leaf plan. execute() constructs a new lazy stream on every call.
#[derive(Debug)]
pub(crate) struct PgScanExec {
    table: PgTable,
    spec: ScanSpec,
    properties: Arc<PlanProperties>,
}

impl PgScanExec {
    pub fn new(table: PgTable, spec: ScanSpec) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(spec.schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table,
            spec,
            properties,
        }
    }
}

impl DisplayAs for PgScanExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PgScanExec: sql={}, bind_count={}",
            self.spec.sql,
            self.spec.parameters.len()
        )
    }
}

impl ExecutionPlan for PgScanExec {
    fn name(&self) -> &str {
        "PgScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan("PgScanExec is a leaf".to_owned()));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "PgScanExec has only partition 0".to_owned(),
            ));
        }
        Ok(self.table.stream(self.spec.clone()))
    }
}
