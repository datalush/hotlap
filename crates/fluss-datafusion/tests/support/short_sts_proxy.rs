// SPDX-License-Identifier: Apache-2.0
//! Test-only STS endpoint requesting genuine 900-second sessions from RustFS.
use std::collections::HashMap;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

pub(super) struct ShortStsProxy(JoinHandle<()>);
impl ShortStsProxy {
    pub(super) async fn start(host: &str, policy: &str) -> io::Result<(Self, String)> {
        let expected: serde_json::Value = serde_json::from_str(policy).map_err(io::Error::other)?;
        let listener = TcpListener::bind((host, 0)).await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let task = tokio::spawn(async move {
            let mut requests = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((client, _)) = accepted else { break };
                        let expected = expected.clone();
                        requests.spawn(async move {
                            let _ = tokio::time::timeout(Duration::from_secs(20), serve(client, expected)).await;
                        });
                    }
                    _ = requests.join_next(), if !requests.is_empty() => {}
                }
            }
        });
        Ok((Self(task), endpoint))
    }
}
impl Drop for ShortStsProxy {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn decode_form(value: &str) -> io::Result<String> {
    let mut output = Vec::new();
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        match byte {
            b'+' => output.push(b' '),
            b'%' => {
                let high = bytes.next().and_then(|b| (b as char).to_digit(16));
                let low = bytes.next().and_then(|b| (b as char).to_digit(16));
                let (Some(high), Some(low)) = (high, low) else {
                    return Err(io::Error::other("invalid form encoding"));
                };
                output.push((high * 16 + low) as u8);
            }
            _ => output.push(byte),
        }
    }
    String::from_utf8(output).map_err(io::Error::other)
}
fn element(name: &str, value: &str) -> String {
    let escaped = value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    format!("<{name}>{escaped}</{name}>")
}
async fn credentials(fields: &HashMap<String, String>) -> io::Result<String> {
    let endpoint = std::env::var("RUSTFS_ENDPOINT").map_err(io::Error::other)?;
    let access = std::env::var("RUSTFS_ACCESS_KEY").map_err(io::Error::other)?;
    let secret = std::env::var("RUSTFS_SECRET_KEY").map_err(io::Error::other)?;
    let mut command = tokio::process::Command::new("aws");
    command
        .args([
            "--endpoint-url",
            &endpoint,
            "sts",
            "assume-role",
            "--role-arn",
            "arn:aws:iam::rustfs:role/fluss-read",
            "--role-session-name",
            fields
                .get("RoleSessionName")
                .map_or("fluss-short-sts", String::as_str),
            "--duration-seconds",
            "900",
            "--policy",
            &fields["Policy"],
            "--output",
            "json",
        ])
        .env("AWS_ACCESS_KEY_ID", access)
        .env("AWS_SECRET_ACCESS_KEY", secret)
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env_remove("AWS_SESSION_TOKEN")
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(15), command.output())
        .await
        .map_err(|_| io::Error::other("test STS request timed out"))??;
    if !output.status.success() {
        return Err(io::Error::other("test STS request failed"));
    }
    let response: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|_| io::Error::other("invalid STS response"))?;
    let mut body = String::from(
        "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><AssumeRoleResult><Credentials>",
    );
    for name in [
        "AccessKeyId",
        "SecretAccessKey",
        "SessionToken",
        "Expiration",
    ] {
        let value = response["Credentials"][name]
            .as_str()
            .ok_or_else(|| io::Error::other("missing STS credential field"))?;
        body.push_str(&element(name, value));
    }
    body.push_str("</Credentials><AssumedRoleUser>");
    for (name, default) in [
        ("AssumedRoleId", "short-sts-test"),
        (
            "Arn",
            "arn:aws:sts::rustfs:assumed-role/fluss-read/short-sts-test",
        ),
    ] {
        body.push_str(&element(
            name,
            response["AssumedRoleUser"][name]
                .as_str()
                .unwrap_or(default),
        ));
    }
    body.push_str("</AssumedRoleUser></AssumeRoleResult><ResponseMetadata><RequestId>short-sts-test</RequestId></ResponseMetadata></AssumeRoleResponse>");
    Ok(body)
}
async fn respond(client: &mut TcpStream, status: &str, body: &str) -> io::Result<()> {
    client.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await
}
async fn serve(mut client: TcpStream, expected: serde_json::Value) -> io::Result<()> {
    let mut request = Vec::new();
    let end = loop {
        if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
            break end;
        }
        if request.len() > 65_536 {
            return respond(&mut client, "400 Bad Request", "invalid request").await;
        }
        let mut buffer = [0; 4096];
        let read = client.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buffer[..read]);
    };
    let header = String::from_utf8_lossy(&request[..end]);
    let size = header
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, size)| size.trim().parse::<usize>().ok())
        .unwrap_or(0);
    if !header.starts_with("POST ") || size == 0 || size > 65_536 {
        return respond(&mut client, "400 Bad Request", "invalid request").await;
    }
    while request.len() < end + 4 + size {
        let mut buffer = [0; 4096];
        let read = client.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buffer[..read]);
    }
    let mut fields = HashMap::new();
    for pair in String::from_utf8_lossy(&request[end + 4..end + 4 + size]).split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            fields.insert(decode_form(key)?, decode_form(value)?);
        }
    }
    let policy = fields
        .get("Policy")
        .and_then(|p| serde_json::from_str::<serde_json::Value>(p).ok());
    if fields.get("Action").map(String::as_str) != Some("AssumeRole")
        || fields.get("RoleArn").map(String::as_str) != Some("arn:aws:iam::rustfs:role/fluss-read")
        || policy.as_ref() != Some(&expected)
    {
        return respond(&mut client, "403 Forbidden", "invalid test policy").await;
    }
    match credentials(&fields).await {
        Ok(body) => {
            eprintln!("short STS: issued 900-second session");
            respond(&mut client, "200 OK", &body).await
        }
        Err(_) => respond(&mut client, "502 Bad Gateway", "test STS request failed").await,
    }
}
