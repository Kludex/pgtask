use std::{error::Error, time::Duration};

use clap::{Parser, Subcommand};
use pgtask::{
    core::{QueueConfig, QueueName, TaskId},
    postgres::Store,
};

#[derive(Parser)]
#[command(name = "pgtask", version, about)]
struct Arguments {
    #[arg(long, env = "PGTASK_DATABASE_URL", hide_env_values = true)]
    database_url: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Health,
    Migrate,
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },
    Cancel {
        task_id: TaskId,
    },
    Retention {
        queue: String,
        #[arg(long, default_value_t = 1_000)]
        limit: u16,
    },
    ConfigureGrants {
        #[arg(long)]
        owner: String,
        #[arg(long)]
        producer: String,
        #[arg(long)]
        worker: String,
        #[arg(long)]
        observer: String,
        #[arg(long)]
        administrator: String,
    },
}

#[derive(Subcommand)]
enum QueueCommand {
    Put {
        name: String,
        #[arg(long, default_value_t = 604_800)]
        terminal_retention_seconds: u64,
        #[arg(long, default_value_t = 2_592_000)]
        idempotency_retention_seconds: u64,
    },
    Pause {
        name: String,
    },
    Resume {
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let arguments = Arguments::parse();
    let store = Store::connect(&arguments.database_url).await?;
    match arguments.command {
        Command::Health => {
            store.health().await?;
            println!("healthy");
        }
        Command::Migrate => {
            store.migrate().await?;
            println!("migrations applied");
        }
        Command::Queue { command } => run_queue_command(&store, command).await?,
        Command::Cancel { task_id } => {
            if store.cancel(task_id).await? {
                println!("task {task_id} cancelled");
            } else {
                println!("task {task_id} is not cancellable");
            }
        }
        Command::Retention { queue, limit } => {
            let queue = QueueName::new(queue)?;
            let tasks = store.delete_expired_terminal(&queue, limit).await?;
            let idempotency_keys = store.delete_expired_idempotency_keys(&queue, limit).await?;
            println!("deleted {tasks} terminal tasks and {idempotency_keys} idempotency keys");
        }
        Command::ConfigureGrants {
            owner,
            producer,
            worker,
            observer,
            administrator,
        } => {
            store
                .configure_grants(&owner, &producer, &worker, &observer, &administrator)
                .await?;
            println!("runtime grants configured");
        }
    }
    Ok(())
}

async fn run_queue_command(store: &Store, command: QueueCommand) -> Result<(), Box<dyn Error>> {
    match command {
        QueueCommand::Put {
            name,
            terminal_retention_seconds,
            idempotency_retention_seconds,
        } => {
            let mut config = QueueConfig::new(QueueName::new(name)?);
            config.terminal_retention = Duration::from_secs(terminal_retention_seconds);
            config.idempotency_retention = Duration::from_secs(idempotency_retention_seconds);
            let queue = store.put_queue(&config).await?;
            println!("queue {} configured", queue.name);
        }
        QueueCommand::Pause { name } => {
            let queue = QueueName::new(name)?;
            if store.set_queue_paused(&queue, true).await?.is_none() {
                return Err(format!("queue {queue} does not exist").into());
            }
            println!("queue {queue} paused");
        }
        QueueCommand::Resume { name } => {
            let queue = QueueName::new(name)?;
            if store.set_queue_paused(&queue, false).await?.is_none() {
                return Err(format!("queue {queue} does not exist").into());
            }
            println!("queue {queue} resumed");
        }
    }
    Ok(())
}
