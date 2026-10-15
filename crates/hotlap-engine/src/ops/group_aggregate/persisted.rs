//! Read-only materialization of persisted aggregate history for restore checks.

use arrow::datatypes::{DataType, Schema};
use hotlap_core::ZSetBatch;

use super::GroupAggregate;
use super::columns::render;
use super::materialize::Change;
use crate::error::EngineError;

impl GroupAggregate {
    /// Materializes authoritative groups without changing or reconstructing them.
    pub(crate) fn persisted_rows(&self) -> Result<Option<ZSetBatch>, EngineError> {
        let Some(schema) = &self.schema else {
            if self.groups.is_empty() {
                return Ok(None);
            }
            return Err(EngineError::Infrastructure(
                "restored groups exist without their schema".into(),
            ));
        };
        let converter = self.converter.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("restored group converter missing".into())
        })?;
        let parser = converter.parser();
        let mut changes = Vec::with_capacity(self.groups.len());
        for (key, entry) in &self.groups {
            validate_group_key(key, schema, &self.key)?;
            converter.convert_rows(vec![parser.parse(key)])?;
            if entry.rows != 0 {
                changes.push(Change {
                    key: key.clone(),
                    previous: None,
                    current: Some(render(&entry.values)?),
                });
            }
        }
        self.materialize(&changes).map(Some)
    }
}

/// Validates the complete encoded key before Arrow's row parser sees it.
/// Eligible maintained group cuts only accept Int32/Int64 equijoin keys.
fn validate_group_key(key: &[u8], schema: &Schema, columns: &[usize]) -> Result<(), EngineError> {
    let mut offset = 0usize;
    for &column in columns {
        let data_type = schema.field(column).data_type();
        let width = match data_type {
            DataType::Int32 => 4,
            DataType::Int64 => 8,
            other => {
                return Err(EngineError::Infrastructure(format!(
                    "unsupported persisted root group key type {other:?}"
                )));
            }
        };
        let end = offset.checked_add(width + 1).ok_or_else(|| {
            EngineError::Infrastructure("restored group key length overflow".into())
        })?;
        let Some(field) = key.get(offset..end) else {
            return Err(invalid_group_key());
        };
        match field[0] {
            0 if field[1..].iter().all(|byte| *byte == 0) => {}
            1 => {}
            _ => return Err(invalid_group_key()),
        }
        offset = end;
    }
    if offset != key.len() {
        return Err(invalid_group_key());
    }
    Ok(())
}

fn invalid_group_key() -> EngineError {
    EngineError::Infrastructure("restored group key encoding is invalid".into())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::validate_group_key;

    #[test]
    fn validates_nullable_fixed_width_key_fields_and_exact_length() {
        let schema = Schema::new(vec![
            Field::new("major", DataType::Int64, true),
            Field::new("minor", DataType::Int32, true),
        ]);
        let valid = [
            &[1][..],
            &[0, 0, 0, 0, 0, 0, 0, 9][..],
            &[0, 0, 0, 0, 0][..],
        ]
        .concat();
        assert!(validate_group_key(&valid, &schema, &[0, 1]).is_ok());

        let malformed = [
            vec![1],
            [vec![1], vec![0; 8], vec![2], vec![0; 4]].concat(),
            [vec![1], vec![0; 8], vec![0], vec![0, 0, 0, 1]].concat(),
            [valid, vec![0]].concat(),
        ];
        for key in malformed {
            assert!(validate_group_key(&key, &schema, &[0, 1]).is_err());
        }
    }
}
