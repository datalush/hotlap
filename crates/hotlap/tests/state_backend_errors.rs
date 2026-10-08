//! Integration tests for state backend error paths: invalid keys and I/O
//! failures must surface as errors, never panics.

mod common;

use std::collections::BTreeMap;
use std::fs;

use common::TempDir;
use hotlap::state::{DurableStateBackend, StateBackend};

#[test]
fn empty_and_invalid_keys_are_rejected() {
    let dir = TempDir::new("invalid");
    let mut durable = DurableStateBackend::open(dir.path()).unwrap();
    let mut memory: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    for backend in [
        &mut durable as &mut dyn StateBackend,
        &mut memory as &mut dyn StateBackend,
    ] {
        assert!(backend.get(b"").is_err(), "empty get");
        assert!(backend.put(b"", b"v".to_vec()).is_err(), "empty put");
        assert!(backend.put(b"a//b", b"v".to_vec()).is_err(), "empty seg");
        assert!(backend.put(b"/a", b"v".to_vec()).is_err(), "leading slash");
    }

    // Arbitrary non-slash bytes remain valid round-trip keys.
    let mut memory: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    StateBackend::put(&mut memory, b"\x00\xff", b"v".to_vec()).unwrap();
    assert_eq!(
        StateBackend::get(&memory, b"\x00\xff").unwrap(),
        Some(b"v".to_vec())
    );
}

#[test]
fn durable_io_errors_are_returned_not_panicked() {
    let dir = TempDir::new("ioerr");
    let mut be = DurableStateBackend::open(dir.path()).unwrap();
    be.put(b"a", b"1".to_vec()).unwrap();

    // `a` is a file, so creating the parent directory for `a/b` must fail.
    let result = be.put(b"a/b", b"2".to_vec());
    assert!(result.is_err(), "parent is a file");
}

#[test]
fn prefix_traversing_file_key_is_absent_like_memory() {
    let dir = TempDir::new("enotdir");
    let mut durable = DurableStateBackend::open(dir.path()).unwrap();
    let mut memory: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    StateBackend::put(&mut memory, b"a", b"1".to_vec()).unwrap();
    durable.put(b"a", b"1".to_vec()).unwrap();

    // Descending through the file-key `a` reads as absent, not an error.
    for prefix in [&b"a/"[..], &b"a/b"[..], &b"a/b/c"[..]] {
        assert_eq!(
            memory.list(prefix).unwrap(),
            durable.list(prefix).unwrap(),
            "list {prefix:?}"
        );
        assert_eq!(
            memory.scan(prefix).unwrap(),
            durable.scan(prefix).unwrap(),
            "scan {prefix:?}"
        );
    }
    for key in [&b"a/b"[..], &b"a/b/c"[..]] {
        assert_eq!(
            StateBackend::get(&memory, key).unwrap(),
            durable.get(key).unwrap(),
            "get {key:?}"
        );
    }
}

#[cfg(unix)]
#[test]
fn durable_read_only_dir_errors() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new("readonly");
    let mut be = DurableStateBackend::open(dir.path()).unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o555)).unwrap();

    let result = be.put(b"key", b"v".to_vec());

    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
    assert!(result.is_err(), "read-only dir must surface an error");
}
