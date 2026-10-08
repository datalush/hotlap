//! Durable local [`StateBackend`]: one file per key under a root directory.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::StateBackend;

const HEX: &[u8; 16] = b"0123456789abcdef";

/// File-per-key state backend rooted at a directory.
///
/// Each key is hex-encoded and stored as `<root>/<hex(key)>`, which keeps
/// arbitrary key bytes file-name safe. A `put` writes `<hex(key)>.tmp` first
/// and then atomically renames it over the final name, so an interrupted write
/// never exposes a half-written value.
///
/// The trait has no error channel, so I/O failures panic. A missing file is a
/// normal `None` from [`StateBackend::get`], not an error.
pub struct DurableStateBackend {
    root: PathBuf,
}

impl DurableStateBackend {
    /// Open a backend rooted at `root`, creating the directory if needed.
    pub fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Directory backing this store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Final on-disk path for `key`.
    fn path_for(&self, key: &[u8]) -> PathBuf {
        self.root.join(hex_encode(key))
    }
}

impl StateBackend for DurableStateBackend {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match fs::read(self.path_for(key)) {
            Ok(value) => Some(value),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => panic!("durable state get failed: {e}"),
        }
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let final_path = self.path_for(key);
        let tmp_path = final_path.with_extension("tmp");
        fs::write(&tmp_path, &value)
            .unwrap_or_else(|e| panic!("durable state temp write failed: {e}"));
        fs::rename(&tmp_path, &final_path)
            .unwrap_or_else(|e| panic!("durable state rename failed: {e}"));
    }

    fn scan(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.list(prefix)
            .into_iter()
            .map(|key| {
                let value = self
                    .get(&key)
                    .expect("listed durable key must still be readable");
                (key, value)
            })
            .collect()
    }

    fn list(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        let entries =
            fs::read_dir(&self.root).unwrap_or_else(|e| panic!("durable state list failed: {e}"));
        let mut keys = Vec::new();
        for entry in entries {
            let entry = entry.unwrap_or_else(|e| panic!("durable state entry failed: {e}"));
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            // Non-hex names (including `.tmp` leftovers) decode to `None` and
            // are ignored; only committed values carry valid hex names.
            let Some(key) = hex_decode(&name) else {
                continue;
            };
            if key.starts_with(prefix) {
                keys.push(key);
            }
        }
        keys.sort();
        keys
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = hex_digit(pair[0])?;
        let lo = hex_digit(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}
