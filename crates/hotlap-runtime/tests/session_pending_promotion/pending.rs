use hotlap::state::StateBackend;

use super::SharedBackend;

pub fn pending_body(valid: &SharedBackend, pending: &SharedBackend) {
    let mut writer = valid.clone();
    for part in ["engine", "sources"] {
        let body = pending
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), body)
            .unwrap();
    }
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
}
