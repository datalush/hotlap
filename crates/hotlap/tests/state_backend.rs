//! Integration tests for the state backend seam: in-memory/durable
//! equivalence and durable persistence across a close/reopen cycle.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use hotlap::state::{DurableStateBackend, StateBackend};

/// Unique scratch directory removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("hotlap-state-{tag}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn sequence() -> Vec<(&'static [u8], &'static [u8])> {
    vec![
        (b"checkpoint/1/state", b"s1"),
        (b"checkpoint/1/offset", b"o1"),
        (b"checkpoint/2/state", b"s2"),
        (b"user/1", b"u1"),
    ]
}

fn seeded_memory() -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut be: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (key, value) in sequence() {
        be.put(key, value.to_vec());
    }
    be
}

fn seeded_durable(root: &Path) -> DurableStateBackend {
    let mut be = DurableStateBackend::open(root).unwrap();
    for (key, value) in sequence() {
        be.put(key, value.to_vec());
    }
    be
}

#[test]
fn memory_and_durable_agree_for_same_sequence() {
    let dir = TempDir::new("agree");
    let memory = seeded_memory();
    let durable = seeded_durable(dir.path());

    for (key, _) in sequence() {
        assert_eq!(
            StateBackend::get(&memory, key),
            durable.get(key),
            "get {key:?}"
        );
    }
    for prefix in [
        &b"checkpoint/"[..],
        &b"checkpoint/1/"[..],
        &b"user/"[..],
        b"",
    ] {
        assert_eq!(
            memory.scan(prefix),
            durable.scan(prefix),
            "scan prefix {prefix:?}"
        );
        assert_eq!(
            memory.list(prefix),
            durable.list(prefix),
            "list prefix {prefix:?}"
        );
    }
}

#[test]
fn durable_persists_across_reopen() {
    let dir = TempDir::new("persist");
    {
        let mut be = DurableStateBackend::open(dir.path()).unwrap();
        be.put(b"checkpoint/1/state", b"payload".to_vec());
    }

    let reopened = DurableStateBackend::open(dir.path()).unwrap();
    assert_eq!(
        reopened.get(b"checkpoint/1/state"),
        Some(b"payload".to_vec())
    );
    assert_eq!(
        reopened.list(b"checkpoint/"),
        vec![b"checkpoint/1/state".to_vec()]
    );
}

#[test]
fn durable_put_is_atomic_and_overwrites() {
    let dir = TempDir::new("atomic");
    let mut be = DurableStateBackend::open(dir.path()).unwrap();
    be.put(b"k", b"v1".to_vec());
    be.put(b"k", b"v2".to_vec());

    assert_eq!(be.get(b"k"), Some(b"v2".to_vec()));
    let names: Vec<String> = fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names.len(), 1, "one file per key, temp renamed away");
    assert!(!names[0].ends_with(".tmp"), "no temp file left behind");
}

#[test]
fn durable_list_ignores_non_hex_leftovers() {
    let dir = TempDir::new("leftover");
    let be = seeded_durable(dir.path());
    fs::write(dir.path().join("stray.tmp"), b"junk").unwrap();

    assert!(be.list(b"").contains(&b"user/1".to_vec()));
    assert!(!be.list(b"").contains(&b"stray.tmp".to_vec()));
    assert_eq!(be.scan(b"checkpoint/").len(), 3);
}
