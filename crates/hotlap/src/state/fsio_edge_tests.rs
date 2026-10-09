//! Edge-case tests that must not touch the real working directory.
//!
//! A relative root resolves its empty anchor to the current directory, so these
//! use an in-memory [`FsOps`] instead of the process CWD.

use std::cell::RefCell;
use std::io;
use std::path::{Path, PathBuf};

use super::fsio::{FsOps, create_dirs_synced_with};

/// In-memory filesystem for relative paths: no real directory is probed or
/// created, so results do not depend on the process working directory.
#[derive(Default)]
struct FakeFs {
    created: RefCell<Vec<PathBuf>>,
    synced: RefCell<Vec<PathBuf>>,
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
        Ok(())
    }

    fn remove_file(&self, _file: &Path) -> io::Result<()> {
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
