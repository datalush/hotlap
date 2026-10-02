// SPDX-License-Identifier: Apache-2.0
//! Execution-time partition discovery and safe string-equality pruning.

use std::collections::HashSet;

use arrow::datatypes::{DataType, Schema};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, Operator};
use fluss::client::FlussConnection;
use fluss::metadata::{PartitionInfo, TablePath};

use crate::metrics::ReportOnce;

pub(crate) const DEFAULT_MAX_ASSIGNED_BUCKETS: usize = 16_384;

/// Conjunctive equality constraints only. Ignoring an unsupported conjunct
/// weakens pruning but never changes the rows DataFusion sees.
#[derive(Clone, Debug, Default)]
pub(crate) struct PartitionFilter(Vec<(String, String)>);

impl PartitionFilter {
    pub(crate) fn from_expr(expr: &Expr, schema: &Schema, keys: &[String]) -> Self {
        let mut values = Vec::new();
        collect_equalities(expr, schema, keys, &mut values);
        Self(values)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn extend(&mut self, other: Self) {
        self.0.extend(other.0);
    }

    pub(crate) fn matches(&self, partition: &PartitionInfo) -> bool {
        let spec = partition.get_partition_spec();
        self.0.iter().all(|(key, value)| {
            spec.get_spec_map()
                .get(key)
                .is_some_and(|actual| actual == value)
        })
    }
}

fn collect_equalities(
    expr: &Expr,
    schema: &Schema,
    keys: &[String],
    values: &mut Vec<(String, String)>,
) {
    let Expr::BinaryExpr(binary) = expr else {
        return;
    };
    if binary.op == Operator::And {
        collect_equalities(&binary.left, schema, keys, values);
        collect_equalities(&binary.right, schema, keys, values);
    } else if binary.op == Operator::Eq {
        let pair = match (&*binary.left, &*binary.right) {
            (Expr::Column(column), Expr::Literal(value, _))
            | (Expr::Literal(value, _), Expr::Column(column)) => Some((column, value)),
            _ => None,
        };
        if let Some((column, datafusion::common::ScalarValue::Utf8(Some(value)))) = pair
            && keys.contains(&column.name)
            && schema
                .field_with_name(&column.name)
                .is_ok_and(|field| field.data_type() == &DataType::Utf8)
        {
            values.push((column.name.clone(), value.clone()));
        }
    }
}

pub(crate) struct PartitionSelection {
    pub(crate) discovered: usize,
    pub(crate) selected: Vec<PartitionInfo>,
    pub(crate) reported: ReportOnce,
}

pub(crate) async fn discover(
    connection: &FlussConnection,
    path: &TablePath,
    filter: &PartitionFilter,
    max_assigned_buckets: usize,
) -> Result<PartitionSelection> {
    let admin = connection.get_admin().map_err(fluss_error)?;
    let mut partitions = admin
        .list_partition_infos(path)
        .await
        .map_err(fluss_error)?;
    let mut seen = HashSet::new();
    for partition in &partitions {
        if !seen.insert(partition.get_partition_id()) {
            return Err(DataFusionError::Execution(format!(
                "Duplicate Fluss partition id {}",
                partition.get_partition_id()
            )));
        }
        validate_bucket_count(partition)?;
    }
    let discovered = partitions.len();
    partitions.retain(|partition| filter.matches(partition));
    validate_bucket_budget(&partitions, max_assigned_buckets)?;
    partitions.sort_unstable_by_key(PartitionInfo::get_partition_id);
    Ok(PartitionSelection {
        discovered,
        selected: partitions,
        reported: ReportOnce::default(),
    })
}

fn validate_bucket_budget(partitions: &[PartitionInfo], limit: usize) -> Result<()> {
    let mut assigned = 0_usize;
    for partition in partitions {
        let count = partition
            .get_bucket_count()
            .expect("partition layout validated") as usize;
        assigned = assigned.checked_add(count).ok_or_else(|| {
            DataFusionError::Execution("Fluss partition bucket count overflowed".into())
        })?;
        if assigned > limit {
            return Err(DataFusionError::Execution(format!(
                "Fluss scan would assign {assigned} buckets (limit {limit}); increase max_assigned_buckets explicitly"
            )));
        }
    }
    Ok(())
}

fn validate_bucket_count(partition: &PartitionInfo) -> Result<()> {
    if !matches!(partition.get_bucket_count(), Some(count) if count > 0) {
        return Err(DataFusionError::Execution(format!(
            "Fluss partition {} has no valid bucket count; refusing an incomplete scan",
            partition.get_partition_name()
        )));
    }
    Ok(())
}

fn fluss_error(error: fluss::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion::logical_expr::{col, lit};
    use fluss::metadata::ResolvedPartitionSpec;
    use std::sync::Arc;

    #[test]
    fn only_safe_conjuncts_prune_partitions() {
        let schema = Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("id", DataType::Int32, true),
        ]);
        let partition = PartitionInfo::new(
            8,
            ResolvedPartitionSpec::new(Arc::from(["region".to_string()]), vec!["north".into()])
                .unwrap(),
        );
        let keys = ["region".to_string()];
        let filter = PartitionFilter::from_expr(
            &col("region").eq(lit("north")).and(col("id").gt(lit(1))),
            &schema,
            &keys,
        );
        assert!(filter.matches(&partition));
        assert!(
            !PartitionFilter::from_expr(&col("region").eq(lit("south")), &schema, &keys)
                .matches(&partition)
        );
        assert!(
            PartitionFilter::from_expr(
                &col("region").eq(lit("south")).or(col("id").eq(lit(1))),
                &schema,
                &keys
            )
            .is_empty()
        );
    }

    #[test]
    fn per_partition_bucket_counts_are_required_for_complete_scans() {
        let partition = PartitionInfo::new(
            9,
            ResolvedPartitionSpec::new(Arc::from(["region".into()]), vec!["north".into()]).unwrap(),
        );
        assert!(validate_bucket_count(&partition).is_err());
        let mut from_server = partition.to_pb();
        for count in [0, -1, 2, 3] {
            from_server.bucket_count = Some(count);
            assert_eq!(
                validate_bucket_count(&PartitionInfo::from_pb(&from_server)).is_ok(),
                count > 0
            );
        }
    }

    #[test]
    fn bucket_budget_rejects_large_scans_without_truncating_partitions() {
        let spec =
            ResolvedPartitionSpec::new(Arc::from(["region".to_owned()]), vec!["north".into()])
                .unwrap();
        let mut pb = PartitionInfo::new(9, spec).to_pb();
        pb.bucket_count = Some(4);
        let partitions = [PartitionInfo::from_pb(&pb)];
        assert!(validate_bucket_budget(&partitions, 3).is_err());
        assert!(validate_bucket_budget(&partitions, 4).is_ok());
    }
}
