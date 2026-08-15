use std::{error::Error, net::SocketAddr};

use axum::http::HeaderName;
use pgtask_web::{AdministratorConfig, application, application_with_administrator};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    let database_url = std::env::var("PGTASK_DATABASE_URL")?;
    let address: SocketAddr = std::env::var("PGTASK_WEB_ADDRESS")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_owned())
        .parse()?;
    let pool = PgPoolOptions::new().max_connections(10).connect(&database_url).await?;
    let administrator = std::env::var("PGTASK_WEB_ADMINISTRATOR")
        .ok()
        .is_some_and(|value| value == "true");
    let application = if administrator {
        let actor_header = std::env::var("PGTASK_WEB_ADMINISTRATOR_ACTOR_HEADER")
            .unwrap_or_else(|_| "x-pgtask-actor".to_owned())
            .parse::<HeaderName>()?;
        application_with_administrator(pool, AdministratorConfig { actor_header })
    } else {
        application(pool)
    };
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, "pgtask UI listening");
    axum::serve(listener, application)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() {
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler installs");
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.expect("SIGINT handler runs"),
        signal = terminate.recv() => assert!(signal.is_some()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.expect("shutdown handler runs");
}
