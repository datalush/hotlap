// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::rpc::error::RpcError;
use std::fmt;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

/// Immutable trust and handshake policy shared across all RPC connections.
#[derive(Clone)]
pub(crate) struct TlsConfig {
    connector: TlsConnector,
    handshake_timeout: Duration,
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConfig")
            .field("handshake_timeout", &self.handshake_timeout)
            .finish_non_exhaustive()
    }
}

impl TlsConfig {
    /// Load trust at client creation; connections after CA rotation need a new client.
    pub(crate) fn new(ca_file: Option<&str>, handshake_timeout_ms: u64) -> Result<Self, String> {
        let mut roots = RootCertStore::empty();
        if let Some(path) = ca_file {
            let certificates = CertificateDer::pem_file_iter(path)
                .map_err(|e| format!("cannot open TLS CA file: {e}"))?;
            for certificate in certificates {
                roots
                    .add(certificate.map_err(|e| format!("invalid TLS CA file: {e}"))?)
                    .map_err(|e| format!("invalid TLS CA certificate: {e}"))?;
            }
        } else {
            let native = rustls_native_certs::load_native_certs();
            for certificate in native.certs {
                // Native stores can contain certificates unsupported by rustls.
                let _ = roots.add(certificate);
            }
        }
        if roots.is_empty() {
            return Err("no trusted TLS CA certificates were loaded".into());
        }
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            handshake_timeout: Duration::from_millis(handshake_timeout_ms),
        })
    }
}

#[derive(Debug)]
pub enum Transport {
    Plain { inner: TcpStream },
    Tls { inner: Box<TlsStream<TcpStream>> },
}

impl AsyncRead for Transport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            Self::Plain { inner } => Pin::new(inner).poll_read(cx, buf),
            Self::Tls { inner } => Pin::new(inner.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.deref_mut() {
            Self::Plain { inner } => Pin::new(inner).poll_write(cx, buf),
            Self::Tls { inner } => Pin::new(inner.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            Self::Plain { inner } => Pin::new(inner).poll_flush(cx),
            Self::Tls { inner } => Pin::new(inner.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            Self::Plain { inner } => Pin::new(inner).poll_shutdown(cx),
            Self::Tls { inner } => Pin::new(inner.as_mut()).poll_shutdown(cx),
        }
    }
}

impl Transport {
    pub(crate) async fn connect(
        server: &str,
        hostname: &str,
        timeout: Option<Duration>,
        tls: Option<&TlsConfig>,
    ) -> Result<Self, RpcError> {
        let tcp_stream = Self::connect_timeout(server, timeout).await?;
        match tls {
            None => Ok(Transport::Plain { inner: tcp_stream }),
            Some(tls) => {
                let name = ServerName::try_from(hostname.to_owned())
                    .map_err(|e| RpcError::Tls(format!("invalid TLS server name: {e}")))?;
                let handshake = tls.connector.connect(name, tcp_stream);
                let stream = tokio::time::timeout(tls.handshake_timeout, handshake)
                    .await
                    .map_err(|_| {
                        RpcError::ConnectionError(format!(
                            "TLS handshake timed out with {hostname}"
                        ))
                    })?
                    .map_err(|e| RpcError::Tls(format!("{hostname}: {e}")))?;
                Ok(Transport::Tls {
                    inner: Box::new(stream),
                })
            }
        }
    }

    async fn connect_timeout(host: &str, timeout: Option<Duration>) -> Result<TcpStream, RpcError> {
        match timeout {
            Some(timeout) => Ok(tokio::time::timeout(timeout, TcpStream::connect(host))
                .await
                .map_err(|_| {
                    RpcError::ConnectionError(format!("Timeout connecting to host {host}"))
                })??),
            None => Ok(TcpStream::connect(host).await?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::ServerConnectionInner;
    use crate::rpc::api_key::ApiKey;
    use crate::rpc::frame::{ReadError, WriteError};
    use crate::rpc::message::{ReadType, RequestBody, WriteType};
    use std::io;
    use std::io::Cursor;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

    fn certificates(hostnames: &[&str]) -> (String, TlsAcceptor) {
        let issued = rcgen::generate_simple_self_signed(
            hostnames
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let key =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(issued.signing_key.serialize_der()));
        let server = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![issued.cert.der().clone()], key)
            .unwrap();
        (issued.cert.pem(), TlsAcceptor::from(Arc::new(server)))
    }

    struct Ping;
    struct Pong;

    impl RequestBody for Ping {
        type ResponseBody = Pong;
        const API_KEY: ApiKey = ApiKey::MetaData;
    }

    impl WriteType<Vec<u8>> for Ping {
        fn write(&self, _: &mut Vec<u8>) -> Result<(), WriteError> {
            Ok(())
        }
    }

    impl ReadType<Cursor<Vec<u8>>> for Pong {
        fn read(_: &mut Cursor<Vec<u8>>) -> Result<Self, ReadError> {
            Ok(Self)
        }
    }

    #[tokio::test]
    async fn framed_rpc_runs_after_tls_handshake() {
        let (pem, acceptor) = certificates(&["bootstrap.fluss.example"]);
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), pem).unwrap();
        let tls = TlsConfig::new(Some(file.path().to_str().unwrap()), 2000).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(socket).await.unwrap();
            assert_eq!(
                stream.get_ref().1.server_name(),
                Some("bootstrap.fluss.example")
            );
            let size = stream.read_u32().await.unwrap();
            let mut request = vec![0; size as usize];
            stream.read_exact(&mut request).await.unwrap();
            let id = &request[4..8];
            stream.write_u32(5).await.unwrap();
            stream.write_all(&[0]).await.unwrap(); // SuccessResponse
            stream.write_all(id).await.unwrap();
            stream.flush().await.unwrap();
        });
        let transport = Transport::connect(
            &address,
            "bootstrap.fluss.example",
            Some(Duration::from_secs(2)),
            Some(&tls),
        )
        .await
        .unwrap();
        let connection =
            ServerConnectionInner::new(BufStream::new(transport), 4096, Arc::from("tls"));
        connection.request(Ping).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn one_address_routes_distinct_hostnames_by_sni() {
        let (pem, acceptor) = certificates(&["coordinator.fluss.example", "tablet.fluss.example"]);
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), pem).unwrap();
        let tls = TlsConfig::new(Some(file.path().to_str().unwrap()), 2000).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let mut names = Vec::new();
            for _ in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                let mut stream = acceptor.accept(socket).await.unwrap();
                names.push(stream.get_ref().1.server_name().unwrap().to_string());
                let mut request = [0; 4];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(&request, b"rpc!");
                stream.write_all(b"done").await.unwrap();
                stream.flush().await.unwrap();
            }
            names
        });

        for name in ["coordinator.fluss.example", "tablet.fluss.example"] {
            let mut stream = Transport::connect(
                &address.to_string(),
                name,
                Some(Duration::from_secs(2)),
                Some(&tls),
            )
            .await
            .unwrap();
            stream.write_all(b"rpc!").await.unwrap();
            stream.flush().await.unwrap();
            let mut response = [0; 4];
            stream.read_exact(&mut response).await.unwrap();
            assert_eq!(&response, b"done");
        }
        assert_eq!(
            server.await.unwrap(),
            ["coordinator.fluss.example", "tablet.fluss.example"]
        );
    }

    #[tokio::test]
    async fn wrong_hostname_and_untrusted_ca_are_rejected() {
        let (pem, acceptor) = certificates(&["correct.fluss.example"]);
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), pem).unwrap();
        let tls = TlsConfig::new(Some(file.path().to_str().unwrap()), 2000).unwrap();
        let other_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            other_file.path(),
            certificates(&["correct.fluss.example"]).0,
        )
        .unwrap();
        let untrusted = TlsConfig::new(Some(other_file.path().to_str().unwrap()), 2000).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                assert!(acceptor.accept(socket).await.is_err());
            }
        });
        let bad_name = Transport::connect(&address, "wrong.fluss.example", None, Some(&tls))
            .await
            .unwrap_err();
        assert!(matches!(bad_name, RpcError::Tls(_)));
        let bad_ca = Transport::connect(&address, "correct.fluss.example", None, Some(&untrusted))
            .await
            .unwrap_err();
        assert!(matches!(bad_ca, RpcError::Tls(_)));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn silent_peer_times_out_before_any_application_data() {
        let (pem, _) = certificates(&["example.fluss.test"]);
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), pem).unwrap();
        let tls = TlsConfig::new(Some(file.path().to_str().unwrap()), 250).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut first = [0];
            socket.read_exact(&mut first).await.unwrap();
            assert_eq!(first[0], 22); // A TLS ClientHello, not a Fluss RPC frame.
            tokio::time::sleep(Duration::from_millis(750)).await;
        });
        let error = Transport::connect(&address, "example.fluss.test", None, Some(&tls))
            .await
            .unwrap_err();
        assert!(
            matches!(error, RpcError::ConnectionError(message) if message.contains("TLS handshake timed out"))
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn plaintext_transport_still_sends_unmodified_bytes() -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 4];
            socket.read_exact(&mut bytes).await.unwrap();
            bytes
        });
        let mut stream = Transport::connect(&address.to_string(), "unused", None, None)
            .await
            .unwrap();
        stream.write_all(b"rpc!").await?;
        assert_eq!(server.await.unwrap(), *b"rpc!");
        Ok(())
    }

    #[test]
    fn tls_requires_readable_nonempty_ca_when_explicit() {
        assert!(TlsConfig::new(Some("/no/such/ca.pem"), 100).is_err());
        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(TlsConfig::new(Some(file.path().to_str().unwrap()), 100).is_err());
    }
}
