//! Edge-case ordering tests that must not touch the real working directory.
//!
//! A relative root resolves its empty anchor to the current directory, so these
//! use an in-memory [`FsOps`] instead of the process CWD. Failure-injection
//! proves error classification, not the behavior of a real power cut.

use std::cell::RefCell;
use std::io;
use std::path::{Path, PathBuf};

use super::fsio::{FsOps, create_dirs_synced_with, remove_file_pruning};

/// In-memory filesystem for relative paths: no real directory is probed or
/// created, so results do not depend on the process working directory.
#[derive(Default)]
struct FakeFs {
    created: RefCell<Vec<PathBuf>>,
    synced: RefCell<Vec<PathBuf>>,
    absent: bool,
    fail_sync: Option<(PathBuf, io::ErrorKind)>,
}

impl FsOps for FakeFs {
    fn exists(&self, _path: &Path) -> bool {
        false
    }

    fn create_dir(&self, dir: &Path) -> io::Result<()> {
        self.created.borrow_mut().push(dir.to_path_buf());
        Ok(())
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.synced.borrow_mut().push(dir.to_path_buf());
        if let Some((path, kind)) = &self.fail_sync
            && path == dir
        {
            return Err(io::Error::from(*kind));
        }
        Ok(())
    }

    fn remove_file(&self, _file: &Path) -> io::Result<()> {
        if self.absent {
            return Err(io::Error::from(io::ErrorKind::NotFound));
        }
        Ok(())
    }

    fn remove_dir(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn create_syncs_dot_for_a_relative_empty_anchor() {
    let ops = FakeFs::default();

    create_dirs_synced_with(&ops, Path::new("a/b")).unwrap();

    assert_eq!(
        &*ops.created.borrow(),
        &[PathBuf::from("a"), PathBuf::from("a/b")]
    );
    // The empty anchor names the current directory, spelled ".".
    assert_eq!(
        &*ops.synced.borrow(),
        &[PathBuf::from("a/b"), PathBuf::from("a"), PathBuf::from(".")]
    );
}

#[test]
fn create_syncs_dot_only_once() {
    let ops = FakeFs::default();

    create_dirs_synced_with(&ops, Path::new(".")).unwrap();

    assert_eq!(&*ops.synced.borrow(), &[PathBuf::from(".")]);
}

#[test]
fn absent_file_is_reported_without_pruning() {
    let ops = FakeFs {
        absent: true,
        ..Default::default()
    };

    let removed = remove_file_pruning(&ops, Path::new("root/a/k"), Path::new("root")).unwrap();

    assert!(!removed, "an absent file reports no removal");
    assert!(ops.synced.borrow().is_empty(), "nothing to sync");
}

#[test]
fn sync_failure_after_delete_propagates_not_absent() {
    let ops = FakeFs {
        fail_sync: Some((PathBuf::from("root/a/b"), io::ErrorKind::NotFound)),
        ..Default::default()
    };

    let error = remove_file_pruning(&ops, Path::new("root/a/b/k"), Path::new("root")).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}
