use std::any::Any;
use std::future::Future;

use fluss::client::FlussAdmin;
use futures::FutureExt;

pub async fn protect_owned_setup<F>(admin: &FlussAdmin, database: &str, setup: F)
where
    F: Future<Output = ()>,
{
    let result = std::panic::AssertUnwindSafe(setup).catch_unwind().await;
    if let Err(payload) = result {
        finish_owned_lifecycle(Err(payload), cleanup_database(admin, database).await);
        unreachable!("failed fixture setup must abort creation");
    }
}

pub async fn cleanup_database(admin: &FlussAdmin, database: &str) -> Result<(), String> {
    admin
        .drop_database(database, true, true)
        .await
        .map_err(|error| error.to_string())?;
    let databases = admin
        .list_databases()
        .await
        .map_err(|error| format!("dropped {database}, but could not verify absence: {error}"))?;
    if databases.iter().any(|candidate| candidate == database) {
        return Err(format!(
            "owned fixture database {database} remains after drop"
        ));
    }
    Ok(())
}

pub fn finish_owned_lifecycle(
    result: Result<(), Box<dyn Any + Send>>,
    cleanup: Result<(), String>,
) {
    match (result, cleanup) {
        (Ok(()), Ok(())) => {}
        (Ok(()), Err(cleanup_error)) => panic!("owned fixture cleanup failed: {cleanup_error}"),
        (Err(payload), Ok(())) => std::panic::resume_unwind(payload),
        (Err(payload), Err(cleanup_error)) => {
            panic!(
                "{}; owned fixture cleanup also failed: {cleanup_error}",
                panic_message(payload.as_ref())
            )
        }
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("fixture setup/test panicked with a non-string payload")
}
