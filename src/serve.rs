use anyhow::{Context, Result};
use axum::{routing::get, Router, Server};
use tracing::info;

use crate::{env, health::get_livez, AppState};

pub async fn serve(state: AppState) -> Result<()> {
    let app = Router::new()
        .route("/livez", get(get_livez))
        .with_state(state);

    let port = env::get_env_var("PORT").unwrap_or_else(|| "3003".to_string());
    info!(port, "server listening");
    let socket_addr = format!("0.0.0.0:{port}").parse().unwrap();

    Server::bind(&socket_addr)
        .serve(app.into_make_service())
        .await
        .context("running server")
}
