use anyhow::Context;
use axum::{routing::get, Router, Server};
use block_submission_archiver::env::{Env, ENV_CONFIG};
use tokio::sync::Notify;
use tracing::{error, info};

use crate::{
    env,
    health::{self},
    AppState,
};

pub async fn serve(state: AppState, shutdown_notify: &Notify) {
    let result = {
        let app = Router::new()
            .route("/livez", get(health::get_livez))
            .with_state(state);

        let address = match ENV_CONFIG.env {
            // This avoids macOS firewall popups when developing locally.
            Env::Dev => "127.0.0.1",
            Env::Stag => "0.0.0.0",
            Env::Prod => "0.0.0.0",
        };

        let port = env::get_env_var("PORT").unwrap_or_else(|| "3003".to_string());

        info!(address, port, "server listening");

        let socket_addr = format!("{address}:{port}").parse().unwrap();

        Server::bind(&socket_addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(async {
                shutdown_notify.notified().await;
            })
            .await
            .context("running server")
    };

    match result {
        Ok(_) => info!("server exited"),
        Err(e) => {
            error!(%e, "server exited with error");
            shutdown_notify.notify_waiters();
        }
    }
}
