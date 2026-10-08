//! Integration tests for the state backend seam: in-memory/durable
//! equivalence, durable persistence, prefix subdirectories, and leftovers.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use common::TempDir;
use hotlap::state::{DurableStateBackend, StateBackend};

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
        StateBackend::put(&mut be, key, value.to_vec()).unwrap();
    }
    be
}

fn seeded_durable(root: &Path) -> DurableStateBackend {
    let mut be = DurableStateBackend::open(root).unwrap();
    for (key, value) in sequence() {
        be.put(key, value.to_vec()).unwrap();
    }
    be
}

fn entries(path: &Path) -> Vec<PathBuf> {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect()
}

#[test]
fn memory_and_durable_agree_for_same_sequence() {
    let dir = TempDir::new("agree");
    let memory = seeded_memory();
    let durable = seeded_durable(dir.path());

    for (key, _) in sequence() {
        assert_eq!(
            StateBackend::get(&memory, key).unwrap(),
            durable.get(key).unwrap(),
            "get {key:?}"
        );
    }
    for prefix in [
        &b"checkpoint"[..],
        &b"checkpoint/"[..],
        &b"checkpoint/1"[..],
        &b"checkpoint/1/"[..],
        &b"user/"[..],
        b"",
    ] {
        assert_eq!(
            memory.scan(prefix).unwrap(),
            durable.scan(prefix).unwrap(),
            "scan prefix {prefix:?}"
        );
        assert_eq!(
            memory.list(prefix).unwrap(),
            durable.list(prefix).unwrap(),
            "list prefix {prefix:?}"
        );
    }
}

#[test]
fn durable_persists_across_reopen() {
    let dir = TempDir::new("persist");
    {
        let mut be = DurableStateBackend::open(dir.path()).unwrap();
        be.put(b"checkpoint/1/state", b"payload".to_vec()).unwrap();
    }

    let reopened = DurableStateBackend::open(dir.path()).unwrap();
    assert_eq!(
        reopened.get(b"checkpoint/1/state").unwrap(),
        Some(b"payload".to_vec())
    );
    assert_eq!(
        reopened.list(b"checkpoint/").unwrap(),
        vec![b"checkpoint/1/state".to_vec()]
    );
}

#[test]
fn durable_creates_and_persists_fresh_namespace() {
    let dir = TempDir::new("fresh");
    {
        let mut be = DurableStateBackend::open(dir.path()).unwrap();
        // A brand-new multi-level namespace must be created and durable.
        be.put(b"brand/new/deep/key", b"v".to_vec()).unwrap();
    }

    let reopened = DurableStateBackend::open(dir.path()).unwrap();
    assert_eq!(
        reopened.get(b"brand/new/deep/key").unwrap(),
        Some(b"v".to_vec())
    );
}

#[test]
fn durable_put_is_atomic_and_overwrites() {
    let dir = TempDir::new("atomic");
    let mut be = DurableStateBackend::open(dir.path()).unwrap();
    be.put(b"key", b"v1".to_vec()).unwrap();
    be.put(b"key", b"v2".to_vec()).unwrap();

    assert_eq!(be.get(b"key").unwrap(), Some(b"v2".to_vec()));
    let names: Vec<String> = entries(dir.path())
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names.len(), 1, "one file per key, temp renamed away");
    assert!(!names[0].ends_with(".tmp"), "no temp file left behind");
}

#[test]
fn durable_uses_prefix_subdirectories() {
    let dir = TempDir::new("ns");
    let be = seeded_durable(dir.path());

    // Two top-level namespaces, each a directory; no flat key files.
    let namespaces = entries(dir.path());
    assert_eq!(namespaces.len(), 2);
    assert!(namespaces.iter().all(|p| p.is_dir()));

    // The checkpoint namespace nests id directories holding leaf files.
    let nested = namespaces
        .iter()
        .find(|p| entries(p).iter().any(|child| child.is_dir()))
        .expect("checkpoint namespace");
    let ids = entries(nested);
    assert_eq!(ids.len(), 2);
    assert!(ids.iter().all(|id| id.is_dir()));
    assert!(
        ids.iter()
            .all(|id| entries(id).iter().all(|leaf| leaf.is_file()))
    );
    assert_eq!(ids.iter().map(|id| entries(id).len()).sum::<usize>(), 3);

    assert_eq!(
        be.list(b"checkpoint/").unwrap(),
        vec![
            b"checkpoint/1/offset".to_vec(),
            b"checkpoint/1/state".to_vec(),
            b"checkpoint/2/state".to_vec(),
        ]
    );
}

#[test]
fn durable_list_ignores_non_hex_leftovers() {
    let dir = TempDir::new("leftover");
    let be = seeded_durable(dir.path());
    fs::write(dir.path().join("stray.tmp"), b"junk").unwrap();

    let all = be.list(b"").unwrap();
    assert!(all.contains(&b"user/1".to_vec()));
    assert!(!all.contains(&b"stray.tmp".to_vec()));
    assert_eq!(be.scan(b"checkpoint/").unwrap().len(), 3);
}
