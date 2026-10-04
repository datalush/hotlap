// SPDX-License-Identifier: Apache-2.0
//! Compose MERGE actions with DataFusion joins, predicates and CASE expressions.

use std::collections::HashSet;
use std::fmt;
use std::ops::Not;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::dml::{MergeIntoAction, MergeIntoClause, MergeIntoClauseKind};
use datafusion::logical_expr::expr::Case;
use datafusion::logical_expr::{
    Expr, ExprSchemable, JoinType, LogicalPlanBuilder, TableType, col, lit,
};
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

pub(crate) const DELETE_FLAG: &str = "__fluss_merge_delete";

struct InputPlan(Arc<dyn ExecutionPlan>);

impl fmt::Debug for InputPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FlussMergeInput")
    }
}

#[async_trait]
impl TableProvider for InputPlan {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(projection) = projection {
            let fields = self.0.schema();
            let exprs: Vec<_> = projection
                .iter()
                .map(|&index| {
                    (
                        Arc::new(PhysicalColumn::new(fields.field(index).name(), index))
                            as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                        fields.field(index).name().clone(),
                    )
                })
                .collect();
            Ok(Arc::new(ProjectionExec::try_new(
                exprs,
                Arc::clone(&self.0),
            )?))
        } else {
            Ok(Arc::clone(&self.0))
        }
    }
}

fn rename(expr: Expr, schema: &DFSchemaRef, target_len: usize) -> Result<Expr> {
    expr.transform_up(|expr| {
        if let Expr::Column(column) = expr {
            let index = schema.index_of_column(&column)?;
            let name = if index < target_len {
                format!("__t{index}")
            } else {
                format!("__s{}", index - target_len)
            };
            Ok(Transformed::yes(col(name)))
        } else {
            Ok(Transformed::no(expr))
        }
    })
    .map(|changed| changed.data)
}

fn choose(conditions: &[(Expr, Expr)], fallback: Expr) -> Expr {
    Expr::Case(Case::new(
        None,
        conditions
            .iter()
            .map(|(cond, value)| (Box::new(cond.clone()), Box::new(value.clone())))
            .collect(),
        Some(Box::new(fallback)),
    ))
}

pub(crate) async fn plan_merge_input(
    state: &dyn Session,
    target: Arc<dyn TableProvider>,
    source: Arc<dyn ExecutionPlan>,
    merge_schema: DFSchemaRef,
    on: Expr,
    clauses: Vec<MergeIntoClause>,
    primary_keys: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    if source.boundedness().is_unbounded() {
        return Err(DataFusionError::NotImplemented(
            "Fluss MERGE requires a finite source; continuous CDC can use upsert/delete primitives"
                .into(),
        ));
    }
    let target_schema = target.schema();
    if target_schema
        .fields()
        .iter()
        .any(|field| field.name() == DELETE_FLAG)
    {
        return Err(DataFusionError::Plan(
            "Fluss MERGE target uses the reserved action column name".into(),
        ));
    }
    let count = target_schema.fields().len();
    if merge_schema.fields().len() != count + source.schema().fields().len() {
        return Err(DataFusionError::Plan(
            "Fluss MERGE source/target schema mismatch".into(),
        ));
    }
    let target_exprs = target_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| col(field.name()).alias(format!("__t{i}")))
        .chain([lit(true).alias("__target_present")]);
    let target = LogicalPlanBuilder::scan("__target", provider_as_source(target), None)?
        .project(target_exprs)?
        .build()?;
    let source_fields = source.schema();
    let source_exprs = source_fields
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| col(field.name()).alias(format!("__s{i}")))
        .chain([lit(true).alias("__source_present")]);
    let source = LogicalPlanBuilder::scan(
        "__source",
        provider_as_source(Arc::new(InputPlan(source))),
        None,
    )?
    .project(source_exprs)?
    .build()?;
    let joined = LogicalPlanBuilder::from(target)
        .join_on(source, JoinType::Full, [rename(on, &merge_schema, count)?])?
        .build()?;
    let mut previous = lit(false);
    let mut actions = Vec::new();
    for clause in clauses {
        let matches = match clause.kind.canonical() {
            MergeIntoClauseKind::Matched => col("__target_present")
                .is_not_null()
                .and(col("__source_present").is_not_null()),
            MergeIntoClauseKind::NotMatchedByTarget => col("__target_present")
                .is_null()
                .and(col("__source_present").is_not_null()),
            MergeIntoClauseKind::NotMatchedBySource => col("__target_present")
                .is_not_null()
                .and(col("__source_present").is_null()),
            MergeIntoClauseKind::NotMatched => unreachable!("canonical clause"),
        };
        let predicate = clause
            .predicate
            .map(|expr| rename(expr, &merge_schema, count))
            .transpose()?
            .unwrap_or_else(|| lit(true));
        let condition = matches.and(predicate.is_true());
        let selected = condition.clone().and(previous.clone().not());
        previous = previous.or(condition);
        let values = match &clause.action {
            MergeIntoAction::Delete => (0..count)
                .map(|i| col(format!("__t{i}")))
                .collect::<Vec<_>>(),
            MergeIntoAction::Update(assignments) => {
                if clause.kind.is_not_matched_by_target() {
                    return Err(DataFusionError::Plan(
                        "UPDATE requires a matched target".into(),
                    ));
                }
                let mut seen = HashSet::new();
                let mut values = (0..count)
                    .map(|i| col(format!("__t{i}")))
                    .collect::<Vec<_>>();
                for (name, value) in assignments {
                    if !seen.insert(name) {
                        return Err(DataFusionError::Plan("Duplicate MERGE assignment".into()));
                    }
                    if primary_keys.contains(name) {
                        return Err(DataFusionError::NotImplemented(
                            "Fluss MERGE cannot update primary/partition key columns".into(),
                        ));
                    }
                    let index = target_schema.index_of(name)?;
                    values[index] = rename(value.clone(), &merge_schema, count)?;
                }
                values
            }
            MergeIntoAction::Insert { columns, values } => {
                if !clause.kind.is_not_matched_by_target() {
                    return Err(DataFusionError::Plan(
                        "INSERT requires an unmatched source".into(),
                    ));
                }
                let names = if columns.is_empty() {
                    target_schema
                        .fields()
                        .iter()
                        .map(|field| field.name().clone())
                        .collect()
                } else {
                    columns.clone()
                };
                if names.len() != values.len() || names.len() != count {
                    return Err(DataFusionError::NotImplemented(
                        "Fluss MERGE INSERT requires every target column exactly once".into(),
                    ));
                }
                let mut used = HashSet::new();
                let mut output = vec![lit(ScalarValue::Null); count];
                for (name, value) in names.iter().zip(values) {
                    if !used.insert(name) {
                        return Err(DataFusionError::Plan(
                            "Duplicate MERGE INSERT column".into(),
                        ));
                    }
                    output[target_schema.index_of(name)?] =
                        rename(value.clone(), &merge_schema, count)?;
                }
                output
            }
        };
        if matches!(clause.action, MergeIntoAction::Delete)
            && clause.kind.is_not_matched_by_target()
        {
            return Err(DataFusionError::Plan(
                "DELETE requires a matched target".into(),
            ));
        }
        actions.push((
            selected,
            values,
            matches!(clause.action, MergeIntoAction::Delete),
        ));
    }
    let mut projected = Vec::new();
    for (index, field) in target_schema.fields().iter().enumerate() {
        let conditions = actions
            .iter()
            .map(|(cond, values, _)| (cond.clone(), values[index].clone()))
            .collect::<Vec<_>>();
        projected.push(
            choose(&conditions, lit(ScalarValue::Null))
                .cast_to(field.data_type(), joined.schema())?
                .alias(field.name()),
        );
    }
    projected.push(
        choose(
            &actions
                .iter()
                .map(|(cond, _, delete)| (cond.clone(), lit(*delete)))
                .collect::<Vec<_>>(),
            lit(false),
        )
        .alias(DELETE_FLAG),
    );
    let active = actions
        .iter()
        .fold(lit(false), |expr, (cond, _, _)| expr.or(cond.clone()));
    let logical = LogicalPlanBuilder::from(joined)
        .filter(active)?
        .project(projected)?
        .build()?;
    // This is a SELECT graph, not another MERGE statement: use the caller's
    // logical optimization and planner without re-entering the DML operation.
    state.create_physical_plan(&logical).await
}
