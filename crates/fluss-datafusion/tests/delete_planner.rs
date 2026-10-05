// SPDX-License-Identifier: Apache-2.0
//! Generic native planner backport regressions, independent of Fluss transport.

use arrow::array::{Int32Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

fn context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_batch(
        "state",
        RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef,
        )])?,
    )?;
    Ok(ctx)
}

async fn ids(ctx: &SessionContext) -> Result<Vec<i32>> {
    Ok(ctx
        .sql("SELECT id FROM state ORDER BY id")
        .await?
        .collect()
        .await?
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect())
}

#[tokio::test]
async fn optimized_empty_delete_never_calls_a_provider_as_unconditional_delete() -> Result<()> {
    let ctx = context()?;
    for predicate in ["FALSE", "1 = 2", "id = NULL"] {
        let result = ctx
            .sql(&format!("DELETE FROM state WHERE {predicate}"))
            .await?
            .collect()
            .await?;
        assert_eq!(
            result[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            0
        );
        assert_eq!(ids(&ctx).await?, [1, 2, 3]);
    }
    let result = ctx
        .sql("DELETE FROM state WHERE id = 2")
        .await?
        .collect()
        .await?;
    assert_eq!(
        result[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(ids(&ctx).await?, [1, 3]);
    Ok(())
}

#[tokio::test]
async fn restrictions_not_representable_by_provider_filters_reject_without_writing() -> Result<()> {
    let ctx = context()?;
    for sql in [
        "DELETE FROM state LIMIT 1",
        "DELETE FROM state WHERE id IN (SELECT id FROM state WHERE id = 2)",
    ] {
        let error = ctx
            .sql(sql)
            .await?
            .collect()
            .await
            .expect_err("filter-only hook cannot honor this plan shape");
        assert!(error.to_string().contains("not supported"), "{error}");
        assert_eq!(ids(&ctx).await?, [1, 2, 3]);
    }
    Ok(())
}

#[tokio::test]
async fn optimized_empty_update_also_preserves_rows_and_reports_zero() -> Result<()> {
    let ctx = context()?;
    for predicate in ["FALSE", "id = NULL"] {
        let result = ctx
            .sql(&format!("UPDATE state SET id = 7 WHERE {predicate}"))
            .await?
            .collect()
            .await?;
        assert_eq!(
            result[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            0
        );
        assert_eq!(ids(&ctx).await?, [1, 2, 3]);
    }
    Ok(())
}
