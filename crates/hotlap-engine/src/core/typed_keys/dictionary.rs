use std::sync::Arc;

use arrow::array::{Array, ArrayRef, new_empty_array};
use arrow::datatypes::{DataType, Field};

use super::compact_array;
use crate::error::EngineError;

pub(super) fn compact_dictionary(
    array: &ArrayRef,
    value_type: &DataType,
    row: usize,
) -> Result<ArrayRef, EngineError> {
    let dictionary = array
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int8Type>>();
    if let Some(dictionary) = dictionary {
        let key =
            arrow::array::PrimitiveArray::<arrow::datatypes::Int8Type>::from(vec![if dictionary
                .keys()
                .is_null(row)
            {
                None
            } else {
                Some(0)
            }]);
        let value_id = dictionary.keys().value(row) as usize;
        let value = if dictionary.keys().is_null(row) {
            new_empty_array(value_type)
        } else {
            compact_array(
                dictionary.values(),
                &Field::new("dictionary_value", value_type.clone(), true),
                value_id,
            )?
        };
        let data =
            arrow::array::DictionaryArray::<arrow::datatypes::Int8Type>::try_new(key, value)?;
        return Ok(Arc::new(data));
    }
    if let Some(dictionary) = array
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int16Type>>()
    {
        let key =
            arrow::array::PrimitiveArray::<arrow::datatypes::Int16Type>::from(vec![if dictionary
                .keys()
                .is_null(row)
            {
                None
            } else {
                Some(0)
            }]);
        let value_id = dictionary.keys().value(row) as usize;
        let value = if dictionary.keys().is_null(row) {
            new_empty_array(value_type)
        } else {
            compact_array(
                dictionary.values(),
                &Field::new("dictionary_value", value_type.clone(), true),
                value_id,
            )?
        };
        return Ok(Arc::new(arrow::array::DictionaryArray::<
            arrow::datatypes::Int16Type,
        >::try_new(key, value)?));
    }
    if let Some(dictionary) = array
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
    {
        let key =
            arrow::array::PrimitiveArray::<arrow::datatypes::Int32Type>::from(vec![if dictionary
                .keys()
                .is_null(row)
            {
                None
            } else {
                Some(0)
            }]);
        let value_id = dictionary.keys().value(row) as usize;
        let value = if dictionary.keys().is_null(row) {
            new_empty_array(value_type)
        } else {
            compact_array(
                dictionary.values(),
                &Field::new("dictionary_value", value_type.clone(), true),
                value_id,
            )?
        };
        return Ok(Arc::new(arrow::array::DictionaryArray::<
            arrow::datatypes::Int32Type,
        >::try_new(key, value)?));
    }
    if let Some(dictionary) = array
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int64Type>>()
    {
        let key =
            arrow::array::PrimitiveArray::<arrow::datatypes::Int64Type>::from(vec![if dictionary
                .keys()
                .is_null(row)
            {
                None
            } else {
                Some(0)
            }]);
        let value_id = dictionary.keys().value(row) as usize;
        let value = if dictionary.keys().is_null(row) {
            new_empty_array(value_type)
        } else {
            compact_array(
                dictionary.values(),
                &Field::new("dictionary_value", value_type.clone(), true),
                value_id,
            )?
        };
        return Ok(Arc::new(arrow::array::DictionaryArray::<
            arrow::datatypes::Int64Type,
        >::try_new(key, value)?));
    }
    if let Some(dictionary) = array
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt8Type>>()
    {
        let key =
            arrow::array::PrimitiveArray::<arrow::datatypes::UInt8Type>::from(vec![if dictionary
                .keys()
                .is_null(row)
            {
                None
            } else {
                Some(0)
            }]);
        let value_id = dictionary.keys().value(row) as usize;
        let value = if dictionary.keys().is_null(row) {
            new_empty_array(value_type)
        } else {
            compact_array(
                dictionary.values(),
                &Field::new("dictionary_value", value_type.clone(), true),
                value_id,
            )?
        };
        return Ok(Arc::new(arrow::array::DictionaryArray::<
            arrow::datatypes::UInt8Type,
        >::try_new(key, value)?));
    }
    if let Some(dictionary) = array
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>()
    {
        let key =
            arrow::array::PrimitiveArray::<arrow::datatypes::UInt16Type>::from(vec![if dictionary
                .keys()
                .is_null(row)
            {
                None
            } else {
                Some(0)
            }]);
        let value_id = dictionary.keys().value(row) as usize;
        let value = if dictionary.keys().is_null(row) {
            new_empty_array(value_type)
        } else {
            compact_array(
                dictionary.values(),
                &Field::new("dictionary_value", value_type.clone(), true),
                value_id,
            )?
        };
        return Ok(Arc::new(arrow::array::DictionaryArray::<
            arrow::datatypes::UInt16Type,
        >::try_new(key, value)?));
    }
    if let Some(dictionary) = array
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt32Type>>()
    {
        let key =
            arrow::array::PrimitiveArray::<arrow::datatypes::UInt32Type>::from(vec![if dictionary
                .keys()
                .is_null(row)
            {
                None
            } else {
                Some(0)
            }]);
        let value_id = dictionary.keys().value(row) as usize;
        let value = if dictionary.keys().is_null(row) {
            new_empty_array(value_type)
        } else {
            compact_array(
                dictionary.values(),
                &Field::new("dictionary_value", value_type.clone(), true),
                value_id,
            )?
        };
        return Ok(Arc::new(arrow::array::DictionaryArray::<
            arrow::datatypes::UInt32Type,
        >::try_new(key, value)?));
    }
    if let Some(dictionary) = array
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt64Type>>()
    {
        let key =
            arrow::array::PrimitiveArray::<arrow::datatypes::UInt64Type>::from(vec![if dictionary
                .keys()
                .is_null(row)
            {
                None
            } else {
                Some(0)
            }]);
        let value_id = dictionary.keys().value(row) as usize;
        let value = if dictionary.keys().is_null(row) {
            new_empty_array(value_type)
        } else {
            compact_array(
                dictionary.values(),
                &Field::new("dictionary_value", value_type.clone(), true),
                value_id,
            )?
        };
        return Ok(Arc::new(arrow::array::DictionaryArray::<
            arrow::datatypes::UInt64Type,
        >::try_new(key, value)?));
    }
    Err(EngineError::Unsupported(
        "unsupported dictionary index type".into(),
    ))
}
