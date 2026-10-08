//! Filesystem helpers for the durable backend: atomic writes, durable
//! directory creation, and absence classification.

use std::fs;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

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
    let missing = missing_ancestors(dir);
    for path in missing.iter().rev() {
        fs::create_dir(path)?;
    }
    for path in &missing {
        sync_dir(path)?;
    }
    Ok(())
}

/// Missing ancestors of `dir`, deepest first. Existing paths stop the walk.
fn missing_ancestors(dir: &Path) -> Vec<PathBuf> {
    let mut missing = Vec::new();
    let mut current = dir;
    while !current.as_os_str().is_empty() && !current.exists() {
        missing.push(current.to_path_buf());
        match current.parent() {
            Some(parent) => current = parent,
            None => break,
        }
    }
    missing
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
