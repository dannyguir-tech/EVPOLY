use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use log::{info, warn};
use serde::Serialize;
use tokio::net::TcpListener;

use polymarket_arbitrage_bot::{
    api::PolymarketApi,
    config::Config,
    market_discovery,
    models::{Market, MarketData},
    monitor::{MarketMonitor, MarketSnapshot},
};

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    discovered: Arc<DiscoveredMarkets>,
    monitor: Arc<MarketMonitor>,
}

#[derive(Debug, Clone, Serialize)]
struct DiscoveredMarkets {
    eth: Market,
    btc: Market,
    solana: Market,
    xrp: Market,
}

#[derive(Debug, Clone, Serialize)]
struct TradingConfigView {
    enable_eth_trading: bool,
    enable_solana_trading: bool,
    enable_xrp_trading: bool,
    check_interval_ms: u64,
    trigger_price: f64,
    min_elapsed_minutes: u64,
    sell_price: f64,
    hold_to_resolution: bool,
}

#[derive(Debug, Clone, Serialize)]
struct SnapshotView {
    observed_at_unix: u64,
    period_timestamp: u64,
    time_remaining_seconds: u64,
    eth_market: MarketData,
    btc_market: MarketData,
    solana_market: MarketData,
    xrp_market: MarketData,
}

#[derive(Debug, Clone, Serialize)]
struct DiscoveryResponse {
    service: &'static str,
    bootstrap_error: Option<String>,
    config: TradingConfigView,
    markets: DiscoveredMarkets,
}

#[derive(Debug, Clone, Serialize)]
struct StateResponse {
    discovery: DiscoveryResponse,
    scan: Option<SnapshotView>,
    scan_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ErrorResponse {
    error: String,
}

fn load_config() -> Config {
    let config_path = PathBuf::from("config.json");
    match std::fs::read_to_string(&config_path) {
        Ok(raw) => serde_json::from_str::<Config>(&raw).unwrap_or_else(|err| {
            warn!("Failed to parse config.json, falling back to defaults: {}", err);
            Config::default()
        }),
        Err(_) => Config::default(),
    }
}

fn config_view(config: &Config) -> TradingConfigView {
    TradingConfigView {
        enable_eth_trading: config.trading.enable_eth_trading,
        enable_solana_trading: config.trading.enable_solana_trading,
        enable_xrp_trading: config.trading.enable_xrp_trading,
        check_interval_ms: config.trading.check_interval_ms,
        trigger_price: config.trading.trigger_price,
        min_elapsed_minutes: config.trading.min_elapsed_minutes,
        sell_price: config.trading.sell_price,
        hold_to_resolution: config.trading.hold_to_resolution,
    }
}

fn snapshot_view(snapshot: MarketSnapshot) -> SnapshotView {
    let observed_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    SnapshotView {
        observed_at_unix,
        period_timestamp: snapshot.period_timestamp,
        time_remaining_seconds: snapshot.time_remaining_seconds,
        eth_market: snapshot.eth_market,
        btc_market: snapshot.btc_market,
        solana_market: snapshot.solana_market,
        xrp_market: snapshot.xrp_market,
    }
}

fn discovery_response(
    config: &Config,
    markets: DiscoveredMarkets,
    bootstrap_error: Option<String>,
) -> DiscoveryResponse {
    DiscoveryResponse {
        service: "evpoly-mcp-bridge",
        bootstrap_error,
        config: config_view(config),
        markets,
    }
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/discovery", get(discovery_handler))
        .route("/scan", get(scan_handler))
        .route("/state", get(state_handler))
        .with_state(state)
}

async fn bootstrap_markets(
    api: &Arc<PolymarketApi>,
    config: &Config,
) -> (DiscoveredMarkets, Option<String>) {
    match market_discovery::get_or_discover_markets(api, config).await {
        Ok((eth, btc, solana, xrp)) => (
            DiscoveredMarkets {
                eth,
                btc,
                solana,
                xrp,
            },
            None,
        ),
        Err(err) => {
            warn!("Market discovery failed, using disabled fallback markets: {}", err);
            (
                DiscoveredMarkets {
                    eth: market_discovery::eth_disabled_fallback_market(),
                    btc: market_discovery::btc_disabled_fallback_market(),
                    solana: market_discovery::solana_disabled_fallback_market(),
                    xrp: market_discovery::xrp_disabled_fallback_market(),
                },
                Some(err.to_string()),
            )
        }
    }
}

async fn bootstrap_state() -> Result<AppState> {
    let config = Arc::new(load_config());

    let api = Arc::new(PolymarketApi::new(
        config.polymarket.gamma_api_url.clone(),
        config.polymarket.clob_api_url.clone(),
        config.polymarket.private_key.clone(),
        config.polymarket.proxy_wallet_address.clone(),
        config.polymarket.signature_type,
    ));

    let (discovered, bootstrap_error) = bootstrap_markets(&api, &config).await;
    if let Some(err) = bootstrap_error.as_ref() {
        warn!("Bridge bootstrapped with fallback markets: {}", err);
    }

    let monitor = Arc::new(
        MarketMonitor::new(
            api,
            discovered.eth.clone(),
            discovered.btc.clone(),
            discovered.solana.clone(),
            discovered.xrp.clone(),
            config.trading.check_interval_ms,
            false,
        )
        .context("failed to initialize market monitor")?,
    );

    Ok(AppState {
        config,
        discovered: Arc::new(discovered),
        monitor,
    })
}

async fn health_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "service": "evpoly-mcp-bridge",
        "status": "ok",
    }))
}

async fn discovery_handler(State(state): State<AppState>) -> Json<DiscoveryResponse> {
    Json(discovery_response(
        state.config.as_ref(),
        (*state.discovered).as_ref().clone(),
        None,
    ))
}

async fn scan_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.monitor.fetch_market_data().await {
        Ok(snapshot) => (StatusCode::OK, Json(snapshot_view(snapshot)).into_response()),
        Err(err) => {
            warn!("Scan snapshot failed: {}", err);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: err.to_string(),
                })
                .into_response(),
            )
        }
    }
}

async fn state_handler(State(state): State<AppState>) -> impl IntoResponse {
    let discovery = discovery_response(state.config.as_ref(), (*state.discovered).as_ref().clone(), None);

    match state.monitor.fetch_market_data().await {
        Ok(snapshot) => (
            StatusCode::OK,
            Json(StateResponse {
                discovery,
                scan: Some(snapshot_view(snapshot)),
                scan_error: None,
            })
            .into_response(),
        ),
        Err(err) => {
            warn!("State snapshot scan failed: {}", err);
            (
                StatusCode::OK,
                Json(StateResponse {
                    discovery,
                    scan: None,
                    scan_error: Some(err.to_string()),
                })
                .into_response(),
            )
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .init();

    let state = bootstrap_state().await?;
    let port = std::env::var("PORT")
        .ok()
        .and_then(|v| v.trim().parse::<u16>().ok())
        .unwrap_or(8080);
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));

    let app = build_router(state);
    let listener = TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("failed to bind bridge listener on {}", bind_addr))?;

    info!("EVPOLY MCP bridge listening on http://{}", bind_addr);
    axum::serve(listener, app)
        .await
        .context("EVPOLY MCP bridge server stopped unexpectedly")?;

    Ok(())
}
