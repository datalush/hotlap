// SPDX-License-Identifier: Apache-2.0
//! Small native source causes; transport/protocol errors remain Fluss errors.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlussScanTimeout {
    pub read: crate::FlussReadCapability,
}
impl std::fmt::Display for FlussScanTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.read {
            crate::FlussReadCapability::Log(_) => {
                f.write_str("Fluss bounded log scan timed out; result is incomplete")
            }
            crate::FlussReadCapability::KvSnapshot => {
                f.write_str("Fluss KV snapshot scan timed out")
            }
        }
    }
}
impl std::error::Error for FlussScanTimeout {}

#[derive(Debug)]
pub struct FlussOperationTimeout {
    pub operation: &'static str,
}
impl std::fmt::Display for FlussOperationTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Fluss streaming {} operation timed out", self.operation)
    }
}
impl std::error::Error for FlussOperationTimeout {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlussReadInvalidation {
    Identity,
    Schema,
    Topology,
    Retention,
}

#[derive(Debug)]
pub struct FlussReadInvalidated {
    pub reason: FlussReadInvalidation,
    pub message: String,
}
impl std::fmt::Display for FlussReadInvalidated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for FlussReadInvalidated {}

pub(crate) fn invalidated(
    reason: FlussReadInvalidation,
    message: impl Into<String>,
) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::External(Box::new(FlussReadInvalidated {
        reason,
        message: message.into(),
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlussWritePhase {
    Preparation,
    Metadata,
    EnqueueAndAck,
}

#[derive(Debug)]
pub struct FlussWriteTimeout {
    pub phase: FlussWritePhase,
}
impl std::fmt::Display for FlussWriteTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.phase {
            FlussWritePhase::Preparation => f.write_str("Fluss write destination preparation timed out before input submission"),
            FlussWritePhase::Metadata => f.write_str("Fluss write metadata operation timed out; earlier batches may already have committed"),
            FlussWritePhase::EnqueueAndAck => f.write_str("Fluss write enqueue/ACK timed out; in-flight writes may have committed"),
        }
    }
}
impl std::error::Error for FlussWriteTimeout {}
