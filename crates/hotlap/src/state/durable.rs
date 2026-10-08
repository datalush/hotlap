//! Durable local [`StateBackend`]: one file per key under prefix subdirectories.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::StateBackend;
use super::StateEntry;
use super::StateError;
use super::fsio::{create_dirs_synced, is_absent, write_atomic};
use super::path::{decode_segment, encode_segment, join_segments, split_prefix, validate_key};

/// File-per-key state backend rooted at a directory.
///
/// Key segments become hex-encoded path components, so a namespace prefix maps
/// to a subdirectory and `scan`/`list` only walk the relevant subtree. `put`
/// writes `<name>.tmp`, fsyncs it, atomically renames it over the final name,
/// then fsyncs the parent directory and any freshly created ancestors, so an
/// interrupted write never exposes a half-written value and a crash does not
/// lose a committed one.
pub struct DurableStateBackend {
    root: PathBuf,
}

impl DurableStateBackend {
    /// Open a backend rooted at `root`, creating the directory if needed.
    pub fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        create_dirs_synced(&root)?;
        Ok(Self { root })
    }

    /// Directory backing this store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// On-disk path for `key`, one hex-encoded component per segment.
    fn path_for(&self, key: &[u8]) -> PathBuf {
        let mut path = self.root.clone();
        for segment in key.split(|b| *b == b'/') {
            path.push(encode_segment(segment));
        }
        path
    }

    /// Directory holding every key that starts with the `dirs` segments.
    fn dir_for(&self, dirs: &[&[u8]]) -> PathBuf {
        let mut path = self.root.clone();
        for segment in dirs {
            path.push(encode_segment(segment));
        }
        path
    }

    /// Keys matching `prefix`, sorted ascending, read from the relevant subtree.
    fn keys_under(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        let Some(parsed) = split_prefix(prefix) else {
            return Ok(Vec::new());
        };
        let start = self.dir_for(&parsed.dirs);
        let mut segments: Vec<Vec<u8>> = parsed.dirs.iter().map(|s| s.to_vec()).collect();
        let mut keys = Vec::new();
        collect_dir(&start, &mut segments, parsed.partial, &mut keys)?;
        keys.sort();
        Ok(keys)
    }
}

impl StateBackend for DurableStateBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        validate_key(key)?;
        let file = self.path_for(key);
        if file.is_dir() {
            return Ok(None);
        }
        match fs::read(&file) {
            Ok(value) => Ok(Some(value)),
            Err(e) if is_absent(e.kind()) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        validate_key(key)?;
        let file = self.path_for(key);
        if file.is_dir() {
            return Err(StateError::InvalidKey);
        }
        if let Some(parent) = file.parent() {
            create_dirs_synced(parent)?;
        }
        write_atomic(&file, &value)?;
        Ok(())
    }

    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        let mut out = Vec::new();
        for key in self.keys_under(prefix)? {
            match fs::read(self.path_for(&key)) {
                Ok(value) => out.push((key, value)),
                // A key listed then removed concurrently is simply skipped.
                Err(e) if is_absent(e.kind()) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(out)
    }

    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        self.keys_under(prefix)
    }
}

/// Recursively collect keys under `dir`. `segments` accumulates decoded
/// segments; `partial` filters the immediate entries (empty means all).
fn collect_dir(
    dir: &Path,
    segments: &mut Vec<Vec<u8>>,
    partial: &[u8],
    out: &mut Vec<Vec<u8>>,
) -> Result<(), StateError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if is_absent(e.kind()) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(segment) = decode_segment(&name) else {
            continue;
        };
        if !partial.is_empty() && !segment.starts_with(partial) {
            continue;
        }
        let is_dir = entry.file_type()?.is_dir();
        segments.push(segment);
        if is_dir {
            collect_dir(&entry.path(), segments, &[], out)?;
        } else {
            out.push(join_segments(segments));
        }
        segments.pop();
    }
    Ok(())
}
