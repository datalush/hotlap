//! Ordering and failure-injection tests for the durable filesystem helpers.
//!
//! These run against a real directory tree and inject sync failures; they prove
//! the order of durable writes, not the behavior of a real power cut.

use std::cell::RefCell;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::fsio::{FsOps, RealFs, create_dirs_synced_with, remove_file_pruning};

/// Scratch directory removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("hotlap-fsio-{tag}-{}-{nanos}", std::process::id());
        let path = std::env::temp_dir().join(name);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Real filesystem with sync calls recorded, and an optional injected failure.
#[derive(Default)]
struct RecordingFs {
    synced: RefCell<Vec<PathBuf>>,
    fail_sync: Option<PathBuf>,
}

impl RecordingFs {
    fn failing_on(path: PathBuf) -> Self {
        Self {
            synced: RefCell::new(Vec::new()),
            fail_sync: Some(path),
        }
    }
}

impl FsOps for RecordingFs {
    fn exists(&self, path: &Path) -> bool {
        RealFs.exists(path)
    }

    fn create_dir(&self, dir: &Path) -> io::Result<()> {
        RealFs.create_dir(dir)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.synced.borrow_mut().push(dir.to_path_buf());
        if self.fail_sync.as_deref() == Some(dir) {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "injected"));
        }
        RealFs.sync_dir(dir)
    }

    fn remove_file(&self, file: &Path) -> io::Result<()> {
        RealFs.remove_file(file)
    }

    fn remove_dir(&self, dir: &Path) -> io::Result<()> {
        RealFs.remove_dir(dir)
    }
}

#[test]
fn create_syncs_new_dirs_then_existing_anchor() {
    let scratch = Scratch::new("create-order");
    let target = scratch.path().join("a").join("b");
    let ops = RecordingFs::default();

    create_dirs_synced_with(&ops, &target).unwrap();

    // New dirs deepest-first, then the existing root that links the subtree.
    assert_eq!(
        &*ops.synced.borrow(),
        &[
            scratch.path().join("a").join("b"),
            scratch.path().join("a"),
            scratch.path().to_path_buf(),
        ]
    );
}

#[test]
fn create_propagates_anchor_sync_failure() {
    let scratch = Scratch::new("create-fail");
    let ops = RecordingFs::failing_on(scratch.path().to_path_buf());

    assert!(create_dirs_synced_with(&ops, &scratch.path().join("a")).is_err());
}

#[test]
fn delete_syncs_non_empty_parent() {
    let scratch = Scratch::new("delete-leaf");
    let dir = scratch.path().join("a");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("k1"), b"v").unwrap();
    fs::write(dir.join("k2"), b"v").unwrap();
    let ops = RecordingFs::default();

    remove_file_pruning(&ops, &dir.join("k1"), scratch.path()).unwrap();

    assert_eq!(&*ops.synced.borrow(), &[dir]);
}

#[test]
fn delete_prunes_empty_dirs_and_syncs_root() {
    let scratch = Scratch::new("delete-prune");
    let dir = scratch.path().join("a").join("b");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("k"), b"v").unwrap();
    let ops = RecordingFs::default();

    remove_file_pruning(&ops, &dir.join("k"), scratch.path()).unwrap();

    assert!(!dir.exists());
    assert_eq!(&*ops.synced.borrow(), &[scratch.path().to_path_buf()]);
}

#[test]
fn delete_propagates_sync_failure() {
    let scratch = Scratch::new("delete-fail");
    let dir = scratch.path().join("a");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("k1"), b"v").unwrap();
    fs::write(dir.join("k2"), b"v").unwrap();
    let ops = RecordingFs::failing_on(dir.clone());

    assert!(remove_file_pruning(&ops, &dir.join("k1"), scratch.path()).is_err());
}
