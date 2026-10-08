//! Smoke test: run a script through the REPL and check its output.

mod support;

use std::sync::Arc;

use hotlap_runtime::{Session, SessionConfig};
use support::{FakeSinkFactory, batch, source_factory};

const SCRIPT: &str = "\
CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s';
CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src GROUP BY k, tumble(_event_time, INTERVAL '10 s');
START;
SELECT k, count FROM mv;
SHOW METRICS;
\\q
SELECT should_not_run;";

#[test]
fn script_prints_tables_metrics_and_quits_cleanly() {
    let data = vec![batch(&[1, 1, 2], &[1000, 2000, 1000])];
    let config = SessionConfig::new()
        .with_source_factory(source_factory(data))
        .with_sink_factory(Arc::new(FakeSinkFactory));
    let mut session = Session::open(config).expect("open session");

    let mut out = Vec::new();
    hotlap_cli::repl::run(&mut session, SCRIPT.as_bytes(), &mut out, false).expect("run repl");
    let text = String::from_utf8(out).expect("utf-8 output");

    assert!(text.contains("START"), "ack output missing:\n{text}");
    assert!(text.contains("count"), "query table missing:\n{text}");
    assert!(
        text.contains("name"),
        "metrics table header missing:\n{text}"
    );
    assert!(
        !text.contains("should_not_run"),
        "\\q did not stop reading input:\n{text}"
    );
    session.shutdown().expect("shutdown");
}
