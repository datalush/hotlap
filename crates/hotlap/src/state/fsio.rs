//! Filesystem helpers for the durable backend: atomic writes, durable
//! directory creation and removal, and absence classification.

use std::fs;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Filesystem mutations the durable backend performs. Behind a trait so tests
/// can observe the order of syncs and force failures against a real tree.
pub(super) trait FsOps {
    fn exists(&self, path: &Path) -> bool;
    fn create_dir(&self, dir: &Path) -> io::Result<()>;
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    fn remove_file(&self, file: &Path) -> io::Result<()>;
    fn remove_dir(&self, dir: &Path) -> io::Result<()>;
}

/// Real filesystem operations.
pub(super) struct RealFs;

impl FsOps for RealFs {
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn create_dir(&self, dir: &Path) -> io::Result<()> {
        fs::create_dir(dir)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        sync_dir(dir)
    }

    fn remove_file(&self, file: &Path) -> io::Result<()> {
        fs::remove_file(file)
    }

    fn remove_dir(&self, dir: &Path) -> io::Result<()> {
        fs::remove_dir(dir)
    }
}

/// Write `value` atomically: temp file, fsync, rename, then fsync the parent.
pub(super) fn write_atomic(path: &Path, value: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(value)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

/// Create `dir` and any missing ancestors, fsyncing every directory that was
/// newly created so a fresh namespace survives a crash. Newly created dirs are
/// synced deepest-first, after their children exist.
pub(super) fn create_dirs_synced(dir: &Path) -> io::Result<()> {
    create_dirs_synced_with(&RealFs, dir)
}

/// [`create_dirs_synced`] against injected [`FsOps`].
///
/// The mapping from the first existing ancestor into the new top directory is a
/// modification of that ancestor, so it is synced after the new dirs. A relative
/// path with no existing component has an empty anchor, which names the current
/// directory and is spelled `.`.
pub(super) fn create_dirs_synced_with(ops: &impl FsOps, dir: &Path) -> io::Result<()> {
    let (missing, anchor) = missing_ancestors(ops, dir);
    if missing.is_empty() {
        return Ok(());
    }
    for path in missing.iter().rev() {
        ops.create_dir(path)?;
    }
    for path in &missing {
        ops.sync_dir(path)?;
    }
    let anchor = if anchor.as_os_str().is_empty() {
        Path::new(".")
    } else {
        anchor.as_path()
    };
    ops.sync_dir(anchor)?;
    Ok(())
}

/// Remove `file` and any emptied ancestor directories.
///
/// Returns `Ok(false)` only when `file` was already absent, so a caller can keep
/// deletion idempotent without mistaking a later error for absence. Every
/// directory that lost an entry is synced right after the entry is gone and
/// before that directory is itself removed; a directory that is removed leaves
/// its own parent modified, so the walk syncs it on the next step. The walk stops
/// at `root`, which is never pruned but is synced when it loses an entry.
pub(super) fn remove_file_pruning(ops: &impl FsOps, file: &Path, root: &Path) -> io::Result<bool> {
    match ops.remove_file(file) {
        Ok(()) => {}
        Err(e) if is_absent(e.kind()) => return Ok(false),
        Err(e) => return Err(e),
    }
    let mut modified = file.parent();
    while let Some(path) = modified {
        // The file or a removed child directory just left `path`.
        ops.sync_dir(path)?;
        if path == root {
            break;
        }
        match ops.remove_dir(path) {
            Ok(()) => modified = path.parent(),
            // A non-empty dir keeps its ancestors non-empty too.
            Err(e) if e.kind() == io::ErrorKind::DirectoryNotEmpty => break,
            // Absence after a delete (or any other failure) is not idempotency.
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

/// Missing ancestors of `dir`, deepest first, plus the first existing ancestor
/// that will link the new subtree.
fn missing_ancestors(ops: &impl FsOps, dir: &Path) -> (Vec<PathBuf>, PathBuf) {
    let mut missing = Vec::new();
    let mut current = dir;
    while !current.as_os_str().is_empty() && !ops.exists(current) {
        missing.push(current.to_path_buf());
        match current.parent() {
            Some(parent) => current = parent,
            None => break,
        }
    }
    (missing, current.to_path_buf())
}

/// Fsync a directory. Some filesystems reject directory fsync; those errors are
/// treated as a no-op so durability does not hinge on an optional feature, while
/// genuine failures are surfaced.
fn sync_dir(dir: &Path) -> io::Result<()> {
    match File::open(dir).and_then(|handle| handle.sync_all()) {
        Ok(()) => Ok(()),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::InvalidInput | io::ErrorKind::Unsupported
            ) =>
        {
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Error kinds that mean "nothing stored here" rather than a real failure.
pub(super) fn is_absent(kind: io::ErrorKind) -> bool {
    matches!(kind, io::ErrorKind::NotFound | io::ErrorKind::NotADirectory)
}
