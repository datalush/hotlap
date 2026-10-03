// SPDX-License-Identifier: Apache-2.0
//! A test-only HTTP/1.1 pass-through proxy to the *existing* RustFS server.
//! It fails only requests carrying an STS session token for the test prefix.

use std::hash::{Hash, Hasher};
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

struct FaultState {
    mode: AtomicU8, // 0 = off, 1 = fail N, 2 = always 503, 3 = hold response
    remaining: AtomicUsize,
    matched: AtomicUsize,
    blocked: AtomicUsize,
    last_session_fingerprint: Mutex<Option<u64>>,
    path_prefix: String,
}

pub(super) struct S3FaultProxy {
    pub(super) endpoint: String,
    state: Arc<FaultState>,
    listener: JoinHandle<()>,
}

impl S3FaultProxy {
    pub(super) async fn start(
        host: &str,
        upstream: &str,
        bucket: &str,
        prefix: &str,
    ) -> io::Result<Self> {
        let target = upstream
            .strip_prefix("http://")
            .ok_or_else(|| io::Error::other("test proxy requires a plain HTTP RustFS endpoint"))?
            .trim_end_matches('/')
            .to_owned();
        let listener = TcpListener::bind((host, 0)).await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(FaultState {
            mode: AtomicU8::new(0),
            remaining: AtomicUsize::new(0),
            matched: AtomicUsize::new(0),
            blocked: AtomicUsize::new(0),
            last_session_fingerprint: Mutex::new(None),
            path_prefix: format!("/{bucket}/{prefix}/log/"),
        });
        let shared = Arc::clone(&state);
        let task = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let state = Arc::clone(&shared);
                let target = target.clone();
                tokio::spawn(async move {
                    let _ = forward(client, &target, &state).await;
                });
            }
        });
        Ok(Self {
            endpoint,
            state,
            listener: task,
        })
    }

    pub(super) fn fail_next(&self, count: usize) {
        self.state.remaining.store(count, Ordering::SeqCst);
        self.state.mode.store(1, Ordering::SeqCst);
    }

    pub(super) fn fail_always(&self) {
        self.state.mode.store(2, Ordering::SeqCst);
    }

    pub(super) fn block(&self) {
        self.state.mode.store(3, Ordering::SeqCst);
    }

    pub(super) fn clear(&self) {
        self.state.mode.store(0, Ordering::SeqCst);
    }

    pub(super) fn matched(&self) -> usize {
        self.state.matched.load(Ordering::SeqCst)
    }

    pub(super) fn blocked(&self) -> usize {
        self.state.blocked.load(Ordering::SeqCst)
    }

    /// Retain only an in-memory fingerprint, never the signed session token.
    pub(super) fn last_session_fingerprint(&self) -> Option<u64> {
        *self.state.last_session_fingerprint.lock().unwrap()
    }
}

impl Drop for S3FaultProxy {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

async fn forward(mut client: TcpStream, target: &str, state: &FaultState) -> io::Result<()> {
    let mut request = Vec::new();
    let end = loop {
        if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
            break end;
        }
        if request.len() > 64 * 1024 {
            return Err(io::Error::other("test proxy request headers too large"));
        }
        let mut buf = [0_u8; 4_096];
        let read = client.read(&mut buf).await?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buf[..read]);
    };
    let header = String::from_utf8_lossy(&request[..end]);
    let first = header.lines().next().unwrap_or_default();
    let method = first.split_whitespace().next().unwrap_or_default();
    let is_temporary_reader = header.lines().any(|line| {
        line.split_once(':')
            .is_some_and(|(key, _)| key.eq_ignore_ascii_case("x-amz-security-token"))
    });
    let candidate = is_temporary_reader
        && matches!(method, "HEAD" | "GET")
        && first.contains(&state.path_prefix)
        && first.contains(".log");

    if candidate {
        if let Some((_, token)) = header
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(key, _)| key.eq_ignore_ascii_case("x-amz-security-token"))
        {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            token.trim().hash(&mut hasher);
            *state.last_session_fingerprint.lock().unwrap() = Some(hasher.finish());
        }
        let mode = state.mode.load(Ordering::SeqCst);
        if mode != 0 {
            state.matched.fetch_add(1, Ordering::SeqCst);
            let fault = match mode {
                1 => state
                    .remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok(),
                2 | 3 => true,
                _ => false,
            };
            if fault && mode == 3 {
                state.blocked.fetch_add(1, Ordering::SeqCst);
                let mut byte = [0_u8; 1];
                let _ = tokio::time::timeout(Duration::from_secs(30), client.read(&mut byte)).await;
                state.blocked.fetch_sub(1, Ordering::SeqCst);
                return Ok(());
            }
            if fault {
                let body =
                    b"<Error><Code>ServiceUnavailable</Code><Message>test fault</Message></Error>";
                client
                    .write_all(
                        format!(
                            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await?;
                client.write_all(body).await?;
                return Ok(());
            }
        }
    }

    // Force a fresh connection per HTTP request, so retry requests also pass
    // through the fault switch. Do not change signed Host or payload bytes.
    let mut upstream = TcpStream::connect(target).await?;
    upstream.write_all(&request[..end + 2]).await?;
    upstream.write_all(b"Connection: close\r\n\r\n").await?;
    upstream.write_all(&request[end + 4..]).await?;
    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
    Ok(())
}
