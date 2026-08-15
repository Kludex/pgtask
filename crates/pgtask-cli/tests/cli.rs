use std::process::Command;

use pgtask::{
    core::{EnqueueRequest, QueueName, TaskId, TaskName},
    postgres::Store,
};
use serde_json::json;

#[test]
fn help_describes_the_administrative_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_pgtask"))
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("health"));
    assert!(stdout.contains("migrate"));
    assert!(stdout.contains("queue"));
    assert!(stdout.contains("cancel"));
    assert!(stdout.contains("retention"));
    assert!(stdout.contains("configure-grants"));

    let output = Command::new(env!("CARGO_BIN_EXE_pgtask"))
        .args(["queue", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("put"));
    assert!(stdout.contains("pause"));
    assert!(stdout.contains("resume"));
}

#[tokio::test]
async fn reports_database_health_through_the_cli() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let output = Command::new(env!("CARGO_BIN_EXE_pgtask"))
        .arg("health")
        .env("PGTASK_DATABASE_URL", database_url)
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "healthy\n");
}

#[tokio::test]
async fn configures_a_queue_through_the_cli() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let queue_name = QueueName::new(format!("cli-{}", TaskId::new())).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_pgtask"))
        .args([
            "queue",
            "put",
            queue_name.as_str(),
            "--terminal-retention-seconds",
            "60",
        ])
        .env("PGTASK_DATABASE_URL", &database_url)
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(
        store
            .get_queue(&queue_name)
            .await
            .unwrap()
            .unwrap()
            .terminal_retention
            .as_secs(),
        60
    );
}

#[tokio::test]
async fn cancels_a_task_through_the_cli() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let task_id = store
        .enqueue(&EnqueueRequest::new(
            TaskName::new(format!("cli-cancel-{}", TaskId::new())).unwrap(),
            json!({}),
        ))
        .await
        .unwrap()
        .task_id;

    let output = Command::new(env!("CARGO_BIN_EXE_pgtask"))
        .args(["cancel", &task_id.to_string()])
        .env("PGTASK_DATABASE_URL", database_url)
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("task {task_id} cancelled\n")
    );
}
