use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt32Array, new_empty_array};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field};

use crate::error::EngineError;

#[path = "dictionary.rs"]
mod dictionary;
use dictionary::compact_dictionary;

pub(super) fn compact_array(
    array: &ArrayRef,
    field: &Field,
    row: usize,
) -> Result<ArrayRef, EngineError> {
    let indices = UInt32Array::from(vec![row as u32]);
    match field.data_type() {
        DataType::Dictionary(_, value_type) => compact_dictionary(array, value_type, row),
        DataType::Utf8View => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .unwrap();
            Ok(Arc::new(arrow::array::StringViewArray::from(vec![
                (!values.is_null(row)).then(|| values.value(row).to_owned()),
            ])))
        }
        DataType::BinaryView => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::BinaryViewArray>()
                .unwrap();
            let value = values.value(row).to_vec();
            Ok(Arc::new(arrow::array::BinaryViewArray::from(vec![
                (!values.is_null(row)).then_some(value.as_slice()),
            ])))
        }
        DataType::Struct(fields) => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .unwrap();
            let children = fields
                .iter()
                .enumerate()
                .map(|(index, child_field)| compact_array(values.column(index), child_field, row))
                .collect::<Result<Vec<_>, _>>()?;
            let nulls = values
                .nulls()
                .map(|nulls| NullBuffer::from(vec![nulls.is_valid(row)]));
            Ok(Arc::new(arrow::array::StructArray::try_new(
                fields.clone(),
                children,
                nulls,
            )?))
        }
        DataType::List(child_field) => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .unwrap();
            let offsets = values.value_offsets();
            let (start, end) = if values.is_null(row) {
                (0, 0)
            } else {
                (offsets[row] as usize, offsets[row + 1] as usize)
            };
            let child_values = compact_range(values.values(), child_field, start, end - start)?;
            let nulls = NullBuffer::from(vec![!values.is_null(row)]);
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, (end - start) as i32]));
            Ok(Arc::new(arrow::array::ListArray::try_new(
                child_field.clone(),
                offsets,
                child_values,
                Some(nulls),
            )?))
        }
        DataType::LargeList(child_field) => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::LargeListArray>()
                .unwrap();
            let offsets = values.value_offsets();
            let (start, end) = if values.is_null(row) {
                (0, 0)
            } else {
                (offsets[row] as usize, offsets[row + 1] as usize)
            };
            let child = compact_range(values.values(), child_field, start, end - start)?;
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, (end - start) as i64]));
            Ok(Arc::new(arrow::array::LargeListArray::try_new(
                child_field.clone(),
                offsets,
                child,
                Some(NullBuffer::from(vec![!values.is_null(row)])),
            )?))
        }
        DataType::FixedSizeList(child_field, size) => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeListArray>()
                .unwrap();
            let start = row
                .checked_mul(*size as usize)
                .ok_or_else(|| EngineError::Infrastructure("fixed-list offset overflow".into()))?;
            let len = *size as usize;
            let child = if values.is_null(row) {
                arrow::array::new_null_array(child_field.data_type(), len)
            } else {
                compact_range(values.values(), child_field, start, len)?
            };
            Ok(Arc::new(arrow::array::FixedSizeListArray::try_new(
                child_field.clone(),
                *size,
                child,
                Some(NullBuffer::from(vec![!values.is_null(row)])),
            )?))
        }
        DataType::ListView(child_field) => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::ListViewArray>()
                .unwrap();
            let start = values.value_offsets()[row] as usize;
            let len = if values.is_null(row) {
                0
            } else {
                values.value_sizes()[row] as usize
            };
            let child_values = compact_range(values.values(), child_field, start, len)?;
            let nulls = NullBuffer::from(vec![!values.is_null(row)]);
            Ok(Arc::new(arrow::array::ListViewArray::try_new(
                child_field.clone(),
                ScalarBuffer::from(vec![0_i32]),
                ScalarBuffer::from(vec![len as i32]),
                child_values,
                Some(nulls),
            )?))
        }
        DataType::LargeListView(child_field) => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::LargeListViewArray>()
                .unwrap();
            let start = values.value_offsets()[row] as usize;
            let len = if values.is_null(row) {
                0
            } else {
                values.value_sizes()[row] as usize
            };
            let child = compact_range(values.values(), child_field, start, len)?;
            Ok(Arc::new(arrow::array::LargeListViewArray::try_new(
                child_field.clone(),
                ScalarBuffer::from(vec![0_i64]),
                ScalarBuffer::from(vec![len as i64]),
                child,
                Some(NullBuffer::from(vec![!values.is_null(row)])),
            )?))
        }
        DataType::Map(entries_field, ordered) => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::MapArray>()
                .unwrap();
            let offsets = values.value_offsets();
            let (start, end) = if values.is_null(row) {
                (0, 0)
            } else {
                (offsets[row] as usize, offsets[row + 1] as usize)
            };
            let entries = values.entries();
            let mut columns = Vec::with_capacity(entries.num_columns());
            for (column, child_field) in entries.fields().iter().enumerate() {
                columns.push(compact_range(
                    entries.column(column),
                    child_field,
                    start,
                    end - start,
                )?);
            }
            let compact_entries =
                arrow::array::StructArray::try_new(entries.fields().clone(), columns, None)?;
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, (end - start) as i32]));
            Ok(Arc::new(arrow::array::MapArray::try_new(
                entries_field.clone(),
                offsets,
                compact_entries,
                Some(NullBuffer::from(vec![!values.is_null(row)])),
                *ordered,
            )?))
        }
        DataType::Union(fields, mode) => compact_union(array, fields, mode, row),
        DataType::RunEndEncoded(run_field, value_field) => {
            compact_run(array, run_field, value_field, row)
        }
        _ => take(array.as_ref(), &indices, None).map_err(Into::into),
    }
}

fn compacted_array(field: &Field, arrays: Vec<ArrayRef>) -> Result<ArrayRef, EngineError> {
    if arrays.is_empty() {
        return Ok(new_empty_array(field.data_type()));
    }
    Ok(arrow::compute::concat(
        &arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>(),
    )?)
}

fn compact_range(
    array: &ArrayRef,
    field: &Field,
    start: usize,
    len: usize,
) -> Result<ArrayRef, EngineError> {
    if let DataType::ListView(child_field) = field.data_type() {
        return compact_list_view_range(array, child_field, start, len);
    }
    if let DataType::LargeListView(child_field) = field.data_type() {
        return compact_large_list_view_range(array, child_field, start, len);
    }
    if let DataType::RunEndEncoded(run_field, value_field) = field.data_type() {
        return compact_run_span(array, run_field, value_field, start, len);
    }
    if len == 0 {
        return Ok(new_empty_array(field.data_type()));
    }
    let indices = UInt32Array::from((start..start + len).map(|i| i as u32).collect::<Vec<_>>());
    let selected = take(array.as_ref(), &indices, None)?;
    let mut compacted = Vec::with_capacity(len);
    for row in 0..len {
        compacted.push(compact_array(&selected, field, row)?);
    }
    compacted_array(field, compacted)
}

fn compact_list_view_range(
    array: &ArrayRef,
    child_field: &Arc<Field>,
    start: usize,
    len: usize,
) -> Result<ArrayRef, EngineError> {
    let source = array
        .as_any()
        .downcast_ref::<arrow::array::ListViewArray>()
        .unwrap();
    let mut row_spans = Vec::with_capacity(len);
    for row in start..start + len {
        if source.is_null(row) {
            row_spans.push(None);
        } else {
            let span_start = source.value_offsets()[row] as usize;
            let span_end = span_start + source.value_sizes()[row] as usize;
            row_spans.push(Some((span_start, span_end)));
        }
    }
    compact_view_spans(source.values(), child_field, row_spans, false)
}

fn compact_large_list_view_range(
    array: &ArrayRef,
    child_field: &Arc<Field>,
    start: usize,
    len: usize,
) -> Result<ArrayRef, EngineError> {
    let source = array
        .as_any()
        .downcast_ref::<arrow::array::LargeListViewArray>()
        .unwrap();
    let mut row_spans = Vec::with_capacity(len);
    for row in start..start + len {
        if source.is_null(row) {
            row_spans.push(None);
        } else {
            let span_start = source.value_offsets()[row] as usize;
            let span_end = span_start + source.value_sizes()[row] as usize;
            row_spans.push(Some((span_start, span_end)));
        }
    }
    compact_view_spans(source.values(), child_field, row_spans, true)
}

fn compact_view_spans(
    source_values: &ArrayRef,
    child_field: &Arc<Field>,
    row_spans: Vec<Option<(usize, usize)>>,
    large: bool,
) -> Result<ArrayRef, EngineError> {
    let len = row_spans.len();
    let mut spans = row_spans
        .iter()
        .flatten()
        .copied()
        .filter(|(span_start, span_end)| span_start < span_end)
        .collect::<Vec<_>>();
    spans.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (span_start, span_end) in spans {
        if let Some(last) = merged.last_mut()
            && span_start <= last.1
        {
            last.1 = last.1.max(span_end);
            continue;
        }
        merged.push((span_start, span_end));
    }
    let mut children = Vec::with_capacity(merged.len());
    let mut bases = Vec::with_capacity(merged.len());
    let mut cursor = 0usize;
    for (span_start, span_end) in &merged {
        bases.push(cursor);
        let child = compact_range(
            source_values,
            child_field,
            *span_start,
            span_end - span_start,
        )?;
        cursor += child.len();
        children.push(child);
    }
    let values = if children.is_empty() {
        new_empty_array(child_field.data_type())
    } else {
        arrow::compute::concat(
            &children
                .iter()
                .map(|child| child.as_ref())
                .collect::<Vec<_>>(),
        )?
    };
    let mut offsets = Vec::with_capacity(len);
    let mut sizes = Vec::with_capacity(len);
    let mut validity = Vec::with_capacity(len);
    for span in row_spans {
        match span {
            None => {
                offsets.push(0_usize);
                sizes.push(0_usize);
                validity.push(false);
            }
            Some((span_start, span_end)) => {
                if span_start == span_end {
                    offsets.push(0_usize);
                    sizes.push(0_usize);
                    validity.push(true);
                    continue;
                }
                let (index, (merged_start, _)) = merged
                    .iter()
                    .enumerate()
                    .find(|(_, (merged_start, merged_end))| {
                        span_start >= *merged_start && span_end <= *merged_end
                    })
                    .expect("each selected ListView span belongs to a merged interval");
                offsets.push(bases[index] + span_start - merged_start);
                sizes.push(span_end - span_start);
                validity.push(true);
            }
        }
    }
    let nulls = Some(NullBuffer::from(validity));
    if large {
        let offsets = offsets
            .into_iter()
            .map(i64::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| EngineError::Infrastructure("large list-view offset overflow".into()))?;
        let sizes = sizes
            .into_iter()
            .map(i64::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| EngineError::Infrastructure("large list-view size overflow".into()))?;
        Ok(Arc::new(arrow::array::LargeListViewArray::try_new(
            child_field.clone(),
            ScalarBuffer::from(offsets),
            ScalarBuffer::from(sizes),
            values,
            nulls,
        )?))
    } else {
        let offsets = offsets
            .into_iter()
            .map(i32::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| EngineError::Infrastructure("list-view offset overflow".into()))?;
        let sizes = sizes
            .into_iter()
            .map(i32::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| EngineError::Infrastructure("list-view size overflow".into()))?;
        Ok(Arc::new(arrow::array::ListViewArray::try_new(
            child_field.clone(),
            ScalarBuffer::from(offsets),
            ScalarBuffer::from(sizes),
            values,
            nulls,
        )?))
    }
}

fn compact_run_span(
    array: &ArrayRef,
    _run_field: &Arc<Field>,
    value_field: &Arc<Field>,
    start: usize,
    len: usize,
) -> Result<ArrayRef, EngineError> {
    macro_rules! compact_span {
        ($index:ty, $ends:ty, $native:ty) => {
            if let Some(run) = array
                .as_any()
                .downcast_ref::<arrow::array::RunArray<$index>>()
            {
                if len == 0 {
                    let ends = <$ends>::from(Vec::<$native>::new());
                    let values = new_empty_array(value_field.data_type());
                    return Ok(Arc::new(arrow::array::RunArray::<$index>::try_new(
                        &ends, &values,
                    )?));
                }
                let first = run.get_physical_index(start);
                let last = run.get_physical_index(start + len - 1);
                let logical_end = start + len;
                let mut ends = Vec::with_capacity(last - first + 1);
                let mut values = Vec::with_capacity(last - first + 1);
                for physical in first..=last {
                    let original_end = run.run_ends().values()[physical] as usize;
                    ends.push((original_end.min(logical_end) - start) as $native);
                    values.push(compact_array(run.values(), value_field, physical)?);
                }
                let ends = <$ends>::from(ends);
                let values = compacted_array(value_field, values)?;
                return Ok(Arc::new(arrow::array::RunArray::<$index>::try_new(
                    &ends, &values,
                )?));
            }
        };
    }
    compact_span!(arrow::datatypes::Int16Type, arrow::array::Int16Array, i16);
    compact_span!(arrow::datatypes::Int32Type, arrow::array::Int32Array, i32);
    compact_span!(arrow::datatypes::Int64Type, arrow::array::Int64Array, i64);
    Err(EngineError::Unsupported(
        "unsupported run-end index type".into(),
    ))
}

fn compact_union(
    array: &ArrayRef,
    fields: &arrow::datatypes::UnionFields,
    mode: &arrow::datatypes::UnionMode,
    row: usize,
) -> Result<ArrayRef, EngineError> {
    let values = array
        .as_any()
        .downcast_ref::<arrow::array::UnionArray>()
        .unwrap();
    let type_id = values.type_id(row);
    let dense = mode == &arrow::datatypes::UnionMode::Dense;
    let selected_offset = values.value_offset(row);
    let mut children = Vec::with_capacity(fields.len());
    for (field_id, field) in fields.iter() {
        if field_id == type_id {
            children.push(compact_array(
                values.child(field_id),
                field,
                selected_offset,
            )?);
        } else if dense {
            children.push(new_empty_array(field.data_type()));
        } else {
            children.push(arrow::array::new_null_array(field.data_type(), 1));
        }
    }
    let type_ids = ScalarBuffer::from(vec![type_id]);
    let offsets = dense.then(|| ScalarBuffer::from(vec![0_i32]));
    Ok(Arc::new(arrow::array::UnionArray::try_new(
        fields.clone(),
        type_ids,
        offsets,
        children,
    )?))
}

fn compact_run(
    array: &ArrayRef,
    _run_field: &Arc<Field>,
    value_field: &Arc<Field>,
    row: usize,
) -> Result<ArrayRef, EngineError> {
    macro_rules! compact_for {
        ($index:ty, $array:ty, $native:ty) => {
            if let Some(values) = array
                .as_any()
                .downcast_ref::<arrow::array::RunArray<$index>>()
            {
                let physical = values.get_physical_index(row);
                let value = compact_array(values.values(), value_field, physical)?;
                let run_ends = <$array>::from(vec![1 as $native]);
                return Ok(Arc::new(arrow::array::RunArray::<$index>::try_new(
                    &run_ends, &value,
                )?));
            }
        };
    }
    compact_for!(arrow::datatypes::Int16Type, arrow::array::Int16Array, i16);
    compact_for!(arrow::datatypes::Int32Type, arrow::array::Int32Array, i32);
    compact_for!(arrow::datatypes::Int64Type, arrow::array::Int64Array, i64);
    Err(EngineError::Unsupported(
        "unsupported run-end index type".into(),
    ))
}
