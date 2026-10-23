#![cfg(feature = "recoverable_integration_tests")]

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use fluss::client::{PreparedLogBatch, RecoverableWriteMode};
use fluss::error::FlussError;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::rpc::message::OffsetSpec;
use fluss_test_cluster::FlussTestingClusterBuilder;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::test]
async fn prepared_batch_restore_retry_and_unknown_writer_error_against_server() {
    let mut builder =
        FlussTestingClusterBuilder::new(short_cluster_name()).with_port(available_port().await);
    let cluster = builder.build().await;
    let connection = cluster.get_fluss_connection().await;
    wait_for_tablet_server_ready(&cluster).await;
    let admin = connection.get_admin().expect("admin");
    let table_path = TablePath::new(
        "fluss",
        format!("prepared_recovery_{}", uuid::Uuid::new_v4().simple()),
    );
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("payload", DataTypes::string())
                .build()
                .expect("schema"),
        )
        .distributed_by(Some(1), vec!["id".to_string()])
        .build()
        .expect("table descriptor");
    admin
        .create_table(&table_path, &descriptor, false)
        .await
        .expect("create table");
    wait_for_table_ready(&admin, &table_path).await;

    let table = connection.get_table(&table_path).await.expect("table");
    let writer = table
        .new_append()
        .expect("append")
        .create_writer()
        .expect("writer");
    let prepared = writer
        .prepare_log_batch(arrow_batch(10, "persist and replay"), 0, 7001, 0)
        .await
        .expect("prepare batch");
    let persisted = prepared.encode();
    let first = writer
        .send_prepared_log_batch(&prepared, RecoverableWriteMode::NewSend)
        .await
        .expect("initial send");
    let restored = PreparedLogBatch::decode(&persisted).expect("restore caller state");
    let replay = writer
        .send_prepared_log_batch(&restored, RecoverableWriteMode::Retry)
        .await
        .expect("replay exact bytes");
    assert_eq!(
        (first.bucket_id, first.base_offset),
        (replay.bucket_id, replay.base_offset)
    );
    let offsets = admin
        .list_offsets(&table_path, &[0], OffsetSpec::Latest)
        .await
        .expect("latest offsets");
    assert_eq!(offsets.get(&0), Some(&1));

    let unknown = writer
        .prepare_log_batch(arrow_batch(11, "unknown writer"), 0, 7002, 0)
        .await
        .expect("prepare unknown writer batch");
    let error = writer
        .send_prepared_log_batch(&unknown, RecoverableWriteMode::Retry)
        .await
        .expect_err("unknown writer retry must fail");
    assert_eq!(
        error.api_error(),
        Some(FlussError::UnknownWriterIdException)
    );
    let offsets = admin
        .list_offsets(&table_path, &[0], OffsetSpec::Latest)
        .await
        .expect("offsets after rejected retry");
    assert_eq!(offsets.get(&0), Some(&1));

    let after_abort = writer
        .prepare_log_batch(arrow_batch(12, "must not dispatch after abort"), 0, 7003, 0)
        .await
        .expect("prepare before abort");
    connection.abort_writes();
    let error = writer
        .send_prepared_log_batch(&after_abort, RecoverableWriteMode::NewSend)
        .await
        .expect_err("closed connection must reject prepared sends");
    assert!(matches!(error, fluss::error::Error::WriterClosed { .. }));
    let offsets = admin
        .list_offsets(&table_path, &[0], OffsetSpec::Latest)
        .await
        .expect("offsets after aborted send");
    assert_eq!(offsets.get(&0), Some(&1));
}

#[tokio::test]
async fn dropped_produce_ack_retries_without_duplicate_append() {
    use fluss::error::Error;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
    let proxy_port = proxy_listener.local_addr().unwrap().port();
    let coordinator_port = available_port_pair().await;
    let tablet_port = coordinator_port + 1;
    let (dropped_ack_tx, dropped_ack_rx) = oneshot::channel();
    let proxy = tokio::spawn(run_drop_one_produce_ack_proxy(
        proxy_listener,
        format!("127.0.0.1:{tablet_port}"),
        dropped_ack_tx,
    ));

    let mut builder = FlussTestingClusterBuilder::new(short_cluster_name())
        .with_port(coordinator_port)
        .with_tablet_advertised_client_port(proxy_port);
    let cluster = builder.build().await;
    let connection = cluster.get_fluss_connection().await;
    wait_for_tablet_server_ready(&cluster).await;
    let admin = connection.get_admin().expect("admin");
    let table_path = TablePath::new(
        "fluss",
        format!("prepared_ack_loss_{}", uuid::Uuid::new_v4().simple()),
    );
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("payload", DataTypes::string())
                .build()
                .expect("schema"),
        )
        .distributed_by(Some(1), vec!["id".to_string()])
        .build()
        .expect("table descriptor");
    admin
        .create_table(&table_path, &descriptor, false)
        .await
        .expect("create table");
    wait_for_table_ready(&admin, &table_path).await;

    let table = connection.get_table(&table_path).await.expect("table");
    let writer = table
        .new_append()
        .expect("append")
        .create_writer()
        .expect("writer");
    let prepared = writer
        .prepare_log_batch(arrow_batch(31, "ack lost after commit"), 0, 7101, 0)
        .await
        .expect("prepare batch");
    let durable_state = prepared.encode();
    let error = writer
        .send_prepared_log_batch(&prepared, RecoverableWriteMode::NewSend)
        .await
        .expect_err("proxy must drop ProduceLog acknowledgment");
    assert!(
        !matches!(error, Error::UnsupportedVersion { .. }),
        "server must support ProduceLog v2: {error}"
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), dropped_ack_rx)
        .await
        .expect("proxy did not see committed response")
        .expect("proxy acknowledgment signal");

    let offsets = admin
        .list_offsets(&table_path, &[0], OffsetSpec::Latest)
        .await
        .expect("latest offset after lost ack");
    assert_eq!(offsets.get(&0), Some(&1));
    let restored = PreparedLogBatch::decode(&durable_state).expect("restore persisted state");
    let replay = writer
        .send_prepared_log_batch(&restored, RecoverableWriteMode::Retry)
        .await
        .expect("retry exact persisted bytes");
    assert_eq!(replay.base_offset, 0);
    let offsets = admin
        .list_offsets(&table_path, &[0], OffsetSpec::Latest)
        .await
        .expect("latest offset after replay");
    assert_eq!(offsets.get(&0), Some(&1));

    proxy.abort();
    drop(cluster);
}

fn short_cluster_name() -> String {
    format!("rpb-{}", &uuid::Uuid::new_v4().simple().to_string()[..8])
}

async fn available_port() -> u16 {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn available_port_pair() -> u16 {
    loop {
        let coordinator = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = coordinator.local_addr().unwrap().port();
        if port < u16::MAX
            && tokio::net::TcpListener::bind(("127.0.0.1", port + 1))
                .await
                .is_ok()
        {
            return port;
        }
    }
}

async fn wait_for_table_ready(admin: &fluss::client::FlussAdmin, table_path: &TablePath) {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if admin
                .list_offsets(table_path, &[0], OffsetSpec::Latest)
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("table bucket did not become ready");
}

async fn wait_for_tablet_server_ready(cluster: &fluss_test_cluster::FlussTestingCluster) {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let connection = cluster.get_fluss_connection().await;
            if connection
                .get_metadata()
                .get_cluster()
                .get_one_available_server()
                .is_some()
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("tablet server did not register with coordinator");
}

fn arrow_batch(id: i32, payload: &str) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![id])),
            Arc::new(StringArray::from(vec![payload])),
        ],
    )
    .expect("Arrow batch")
}

async fn run_drop_one_produce_ack_proxy(
    listener: TcpListener,
    upstream: String,
    dropped_ack: oneshot::Sender<()>,
) {
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let drop_next_ack = Arc::new(AtomicBool::new(true));
    let pending_produce_id = Arc::new(AtomicI32::new(-1));
    let dropped_ack = Arc::new(Mutex::new(Some(dropped_ack)));
    loop {
        let (client, _) = listener.accept().await.unwrap();
        let server = TcpStream::connect(&upstream).await.unwrap();
        let (client_read, mut client_write) = client.into_split();
        let (mut server_read, server_write) = server.into_split();
        let mut request_forwarder = {
            let pending_id = Arc::clone(&pending_produce_id);
            tokio::spawn(async move {
                let mut client_read = client_read;
                let mut server_write = server_write;
                loop {
                    let length = match client_read.read_i32().await {
                        Ok(length) => length,
                        Err(_) => return,
                    };
                    let mut frame = vec![0; usize::try_from(length).unwrap()];
                    if client_read.read_exact(&mut frame).await.is_err() {
                        return;
                    }
                    if frame.len() >= 8 && i16::from_be_bytes([frame[0], frame[1]]) == 1014 {
                        pending_id.store(
                            i32::from_be_bytes(frame[4..8].try_into().unwrap()),
                            Ordering::SeqCst,
                        );
                    }
                    if server_write.write_i32(length).await.is_err()
                        || server_write.write_all(&frame).await.is_err()
                        || server_write.flush().await.is_err()
                    {
                        return;
                    }
                }
            })
        };
        let response_forwarder = {
            let drop_next_ack = Arc::clone(&drop_next_ack);
            let pending_id = Arc::clone(&pending_produce_id);
            let dropped_ack = Arc::clone(&dropped_ack);
            async move {
                loop {
                    let length = match server_read.read_i32().await {
                        Ok(length) => length,
                        Err(_) => return,
                    };
                    let mut frame = vec![0; usize::try_from(length).unwrap()];
                    if server_read.read_exact(&mut frame).await.is_err() {
                        return;
                    }
                    if frame.len() >= 5 {
                        let response_id = i32::from_be_bytes(frame[1..5].try_into().unwrap());
                        if pending_id.load(Ordering::SeqCst) == response_id
                            && drop_next_ack.swap(false, Ordering::SeqCst)
                        {
                            if let Some(signal) = dropped_ack.lock().unwrap().take() {
                                let _ = signal.send(());
                            }
                            let _ = client_write.shutdown().await;
                            return;
                        }
                    }
                    if client_write.write_i32(length).await.is_err()
                        || client_write.write_all(&frame).await.is_err()
                        || client_write.flush().await.is_err()
                    {
                        return;
                    }
                }
            }
        };
        tokio::select! { _ = &mut request_forwarder => {}, _ = response_forwarder => request_forwarder.abort(), }
    }
}
