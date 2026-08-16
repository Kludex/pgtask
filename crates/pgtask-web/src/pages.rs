use std::fmt::Write;

use chrono::{DateTime, Utc};
use html_escape::{encode_double_quoted_attribute, encode_text};

use crate::model::{
    Dashboard, QueueSummary, ScheduleDetail, ScheduleSummary, TaskDetail, TaskSummary, WorkerDetail, WorkerSummary,
};

const STYLE: &str = "body{font:15px system-ui;max-width:1200px;margin:0 auto;padding:24px;color:#17202a}\
nav{display:flex;gap:16px;margin-bottom:24px}a{color:#075985}table{width:100%;border-collapse:collapse;margin:12px 0 28px}\
th,td{text-align:left;padding:8px;border-bottom:1px solid #d5d8dc;vertical-align:top}th{font-size:13px;color:#566573}\
code,pre{font:13px ui-monospace,monospace}pre{white-space:pre-wrap;background:#f4f6f7;padding:12px;border-radius:6px}\
.state{font-weight:600}.muted{color:#707b7c}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:16px}\
.card{border:1px solid #d5d8dc;border-radius:8px;padding:16px}.card h2{margin-top:0}input{padding:8px;width:min(420px,80%)}\
button{padding:8px;margin-right:8px}.actions{display:flex;gap:8px}";

fn layout(title: &str, body: &str) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" \
         content=\"width=device-width,initial-scale=1\"><title>{}</title><style>{STYLE}</style></head><body>\
         <nav><strong>pgtask</strong><a href=\"/\">Queues</a><a href=\"/tasks\">Tasks</a>\
         <a href=\"/schedules\">Schedules</a><a href=\"/workers\">Workers</a></nav>{body}</body></html>",
        encode_text(title),
    )
}

fn time(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn optional_time(value: Option<DateTime<Utc>>) -> String {
    value.map_or_else(|| "-".to_owned(), time)
}

fn queue_rows(queues: &[QueueSummary]) -> String {
    let mut rows = String::new();
    for queue in queues {
        let status = if queue.paused_at.is_some() { "paused" } else { "active" };
        write!(
            rows,
            "<tr><td>{}</td><td class=\"state\">{status}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>\
             <td>{}</td><td>{}</td><td>{}</td></tr>",
            encode_text(&queue.name),
            queue.pending_count,
            queue.ready_count,
            queue.routable_count,
            queue.unroutable_count,
            queue.running_count,
            queue.waiting_count,
            queue.terminal_count,
        )
        .unwrap();
    }
    rows
}

fn task_rows(tasks: &[TaskSummary]) -> String {
    let mut rows = String::new();
    for task in tasks {
        write!(
            rows,
            "<tr><td><a href=\"/tasks/{}\"><code>{}</code></a></td><td>{}</td><td>{}</td>\
             <td class=\"state\">{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            task.id,
            task.id,
            encode_text(&task.queue_name),
            encode_text(&task.task_name),
            encode_text(&task.state),
            task.attempt,
            time(task.created_at),
            optional_time(task.completed_at),
        )
        .unwrap();
    }
    rows
}

fn schedule_rows(schedules: &[ScheduleSummary]) -> String {
    let mut rows = String::new();
    for schedule in schedules {
        let status = if schedule.paused_at.is_some() {
            "paused"
        } else {
            "active"
        };
        write!(
            rows,
            "<tr><td><a href=\"/schedules/{}\">{}</a></td><td>{}</td><td>{}</td><td>{}</td>\
             <td>{}</td><td class=\"state\">{status}</td></tr>",
            schedule.id,
            encode_text(&schedule.name),
            encode_text(&schedule.kind),
            encode_text(&schedule.queue_name),
            encode_text(&schedule.task_name),
            time(schedule.next_run_at),
        )
        .unwrap();
    }
    rows
}

fn worker_rows(workers: &[WorkerSummary]) -> String {
    let mut rows = String::new();
    for worker in workers {
        let status = if worker.live { "live" } else { "expired" };
        write!(
            rows,
            "<tr><td><a href=\"/workers/{}\"><code>{}</code></a></td><td>{}</td><td>{}</td>\
             <td class=\"state\">{status}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            worker.id,
            worker.id,
            encode_text(&worker.queue_name),
            encode_text(&worker.version),
            worker.draining,
            time(worker.heartbeat_at),
            time(worker.expires_at),
        )
        .unwrap();
    }
    rows
}

pub fn dashboard(data: &Dashboard) -> String {
    let body = format!(
        "<h1>Queues</h1><table><thead><tr><th>Queue</th><th>Status</th><th>Pending</th><th>Ready</th>\
         <th>Routable</th><th>Unroutable</th><th>Running</th><th>Waiting</th><th>Terminal</th></tr></thead>\
         <tbody>{}</tbody></table>\
         <div class=\"grid\"><div class=\"card\"><h2>Recent tasks</h2><p>{} visible</p><a href=\"/tasks\">Inspect tasks</a></div>\
         <div class=\"card\"><h2>Schedules</h2><p>{} configured</p><a href=\"/schedules\">Inspect schedules</a></div>\
         <div class=\"card\"><h2>Workers</h2><p>{} registered</p><a href=\"/workers\">Inspect workers</a></div></div>",
        queue_rows(&data.queues),
        data.tasks.len(),
        data.schedules.len(),
        data.workers.len(),
    );
    layout("pgtask queues", &body)
}

pub fn tasks(tasks: &[TaskSummary], query: Option<&str>) -> String {
    let query = encode_double_quoted_attribute(query.unwrap_or_default());
    let body = format!(
        "<h1>Tasks</h1><form method=\"get\"><input name=\"query\" value=\"{query}\" \
         placeholder=\"Task name or task ID\"><button type=\"submit\">Search</button></form>\
         <table><thead><tr><th>ID</th><th>Queue</th><th>Task</th><th>State</th><th>Attempt</th>\
         <th>Created</th><th>Completed</th></tr></thead><tbody>{}</tbody></table>",
        task_rows(tasks),
    );
    layout("pgtask tasks", &body)
}

pub fn task(task: &TaskDetail, administrator: bool) -> String {
    let mut attempt_rows = String::new();
    for attempt in &task.attempts {
        write!(
            attempt_rows,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td><pre>{}</pre></td></tr>",
            attempt.number,
            encode_text(&attempt.state),
            time(attempt.started_at),
            optional_time(attempt.finished_at),
            encode_text(attempt.error.as_deref().unwrap_or("")),
        )
        .unwrap();
    }
    let mut checkpoint_rows = String::new();
    for checkpoint in &task.checkpoints {
        write!(
            checkpoint_rows,
            "<tr><td>{}</td><td>{}</td><td><pre>{}</pre></td><td>{}</td></tr>",
            encode_text(&checkpoint.step_name),
            checkpoint.occurrence,
            encode_text(&checkpoint.value),
            time(checkpoint.created_at),
        )
        .unwrap();
    }
    let mut signal_rows = String::new();
    for signal in &task.signals {
        write!(
            signal_rows,
            "<tr><td>{}</td><td>{}</td><td><pre>{}</pre></td><td>{}</td></tr>",
            encode_text(&signal.name),
            signal.occurrence,
            encode_text(&signal.value),
            time(signal.created_at),
        )
        .unwrap();
    }
    let mut audit_rows = String::new();
    for event in &task.administrator_audit {
        write!(
            audit_rows,
            "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
            encode_text(&event.action),
            encode_text(&event.actor),
            time(event.occurred_at),
        )
        .unwrap();
    }
    let actions = if administrator {
        format!(
            "<h2>Administrator actions</h2><div class=\"actions\">\
             <form method=\"post\" action=\"/admin/tasks/{}/cancel\"><button type=\"submit\">Cancel</button></form>\
             <form method=\"post\" action=\"/admin/tasks/{}/retry\"><button type=\"submit\">Retry</button></form></div>",
            task.id, task.id,
        )
    } else {
        String::new()
    };
    let body = format!(
        "<h1><code>{}</code></h1><p><strong>{}</strong> on {} - {} v{} - attempt {}/{}</p>\
         <p class=\"muted\">Run {} - created {} - completed {}</p><h2>Payload</h2><pre>{}</pre>\
         <h2>Result</h2><pre>{}</pre><h2>Error</h2><pre>{}</pre>\
         <h2>Attempts</h2><table><tr><th>Attempt</th><th>State</th><th>Started</th><th>Finished</th><th>Error</th></tr>{attempt_rows}</table>\
         <h2>Checkpoints</h2><table><tr><th>Step</th><th>Occurrence</th><th>Value</th><th>Created</th></tr>{checkpoint_rows}</table>\
         <h2>Signals</h2><table><tr><th>Name</th><th>Occurrence</th><th>Value</th><th>Created</th></tr>{signal_rows}</table>\
         <h2>Administrator audit</h2><table><tr><th>Action</th><th>Actor</th><th>Time</th></tr>{audit_rows}</table>{actions}",
        task.id,
        encode_text(&task.state),
        encode_text(&task.queue_name),
        encode_text(&task.task_name),
        task.handler_version,
        task.attempt,
        task.max_attempts,
        time(task.run_at),
        time(task.created_at),
        optional_time(task.completed_at),
        encode_text(&task.payload),
        encode_text(task.result.as_deref().unwrap_or("")),
        encode_text(task.error.as_deref().unwrap_or("")),
    );
    layout("pgtask task", &body)
}

pub fn schedules(schedules: &[ScheduleSummary]) -> String {
    let body = format!(
        "<h1>Schedules</h1><table><tr><th>Name</th><th>Kind</th><th>Queue</th><th>Task</th><th>Next run</th>\
         <th>Status</th></tr>{}</table>",
        schedule_rows(schedules),
    );
    layout("pgtask schedules", &body)
}

pub fn schedule(detail: &ScheduleDetail, administrator: bool) -> String {
    let mut rows = String::new();
    for occurrence in &detail.occurrences {
        write!(
            rows,
            "<tr><td>{}</td><td><a href=\"/tasks/{}\"><code>{}</code></a></td><td>{}</td><td>{}</td><td>{}</td></tr>",
            time(occurrence.scheduled_for),
            occurrence.task_id,
            occurrence.task_id,
            encode_text(&occurrence.state),
            time(occurrence.created_at),
            optional_time(occurrence.completed_at),
        )
        .unwrap();
    }
    let mut audit_rows = String::new();
    for event in &detail.administrator_audit {
        write!(
            audit_rows,
            "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
            encode_text(&event.action),
            encode_text(&event.actor),
            time(event.occurred_at),
        )
        .unwrap();
    }
    let schedule = &detail.schedule;
    let actions = if administrator {
        format!(
            "<h2>Administrator actions</h2><div class=\"actions\">\
             <form method=\"post\" action=\"/admin/schedules/{}/pause\"><button type=\"submit\">Pause</button></form>\
             <form method=\"post\" action=\"/admin/schedules/{}/resume\"><button type=\"submit\">Resume</button></form></div>",
            schedule.id, schedule.id,
        )
    } else {
        String::new()
    };
    let body = format!(
        "<h1>{}</h1><p>{} schedule for {} on {}. Next run: {}.</p><h2>Occurrences</h2>\
         <table><tr><th>Scheduled for</th><th>Task</th><th>State</th><th>Created</th><th>Completed</th></tr>{rows}</table>\
         <h2>Administrator audit</h2><table><tr><th>Action</th><th>Actor</th><th>Time</th></tr>{audit_rows}</table>{actions}",
        encode_text(&schedule.name),
        encode_text(&schedule.kind),
        encode_text(&schedule.task_name),
        encode_text(&schedule.queue_name),
        time(schedule.next_run_at),
    );
    layout("pgtask schedule", &body)
}

pub fn workers(workers: &[WorkerSummary]) -> String {
    let body = format!(
        "<h1>Workers</h1><table><tr><th>ID</th><th>Queue</th><th>Version</th><th>Status</th><th>Draining</th>\
         <th>Heartbeat</th><th>Expires</th></tr>{}</table>",
        worker_rows(workers),
    );
    layout("pgtask workers", &body)
}

pub fn worker(detail: &WorkerDetail) -> String {
    let mut rows = String::new();
    for capability in &detail.capabilities {
        write!(
            rows,
            "<tr><td>{}</td><td>{}</td></tr>",
            encode_text(&capability.task_name),
            capability.handler_version,
        )
        .unwrap();
    }
    let worker = &detail.worker;
    let body = format!(
        "<h1><code>{}</code></h1><p>{} worker on {}. Version {}. Draining: {}.</p>\
         <p class=\"muted\">Heartbeat {} - expires {}</p><h2>Capabilities</h2>\
         <table><tr><th>Task</th><th>Version</th></tr>{rows}</table>",
        worker.id,
        if worker.live { "Live" } else { "Expired" },
        encode_text(&worker.queue_name),
        encode_text(&worker.version),
        worker.draining,
        time(worker.heartbeat_at),
        time(worker.expires_at),
    );
    layout("pgtask worker", &body)
}

pub fn error(status: u16, message: &str) -> String {
    layout(
        "pgtask error",
        &format!("<h1>{status}</h1><p>{}</p>", encode_text(message)),
    )
}
