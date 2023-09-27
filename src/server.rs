use anyhow::Context;
use axum::{routing::get, Router, Server};
use tokio::sync::Notify;
use tracing::{error, info};

use crate::{
    env::{self, Env, ENV_CONFIG},
    health::{self, RedisConsumerHealth, RedisHealth},
};

#[derive(Clone)]
pub struct AppState {
    pub redis_consumer_health: RedisConsumerHealth,
    pub redis_health: RedisHealth,
}

pub async fn serve(
    redis_consumer_health: RedisConsumerHealth,
    redis_health: RedisHealth,
    shutdown_notify: &Notify,
) {
    let result = {
        let state = AppState {
            redis_consumer_health,
            redis_health,
        };

        let app = Router::new()
            .route("/livez", get(health::get_livez))
            .with_state(state);

        let address = match ENV_CONFIG.env {
            // Developing locally we don't want to expose our server to the world.
            // This also avoids the macOS firewall prompt.
            Env::Dev => "127.0.0.1",
            Env::Stag | Env::Prod => "0.0.0.0",
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
        Ok(_) => info!("server thread exiting"),
        Err(e) => {
            error!(%e, "server thread hit error, exiting");
            shutdown_notify.notify_waiters();
        }
    }
}
