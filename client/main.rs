pub mod forwarder;
pub mod grpc_client;
pub mod metrics;
pub mod quic_forwarder;
pub mod rpc_forwarder;

use crate::forwarder::spawn_forwarder;
use crate::grpc_client::spawn_grpc_client;
use env_logger::Env;
use log::{error, info};
use solana_sdk::signature::read_keypair_file;
use structopt::StructOpt;
use tokio::time::{sleep, Duration};
use tonic::transport::Uri;

pub const VERSION: &str = "rust-0.0.7-beta";

#[derive(Debug, StructOpt)]
struct Params {
    #[structopt(long = "tls-grpc-ca-cert")]
    tls_grpc_ca_cert: Option<String>,

    #[structopt(long = "tls-grpc-client-key")]
    tls_grpc_client_key: Option<String>,

    #[structopt(long = "tls-grpc-client-cert")]
    tls_grpc_client_cert: Option<String>,

    #[structopt(long = "grpc-url", default_value = "http://127.0.0.1:50051")]
    grpc_urls: Vec<String>,

    #[structopt(long = "identity")]
    identity: Option<String>,

    #[structopt(long = "tpu-addr")]
    tpu_addr: Option<String>,

    #[structopt(long = "metrics-addr", default_value = "127.0.0.1:9091")]
    metrics_addr: String,

    #[structopt(long = "rpc-url")]
    rpc_url: Option<String>,

    #[structopt(long = "blackhole")]
    blackhole: bool,

    #[structopt(long = "throttle-parallel", default_value = "1000")]
    throttle_parallel: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let params = Params::from_args();

    let (identity, tpu_addr) = match (&params.identity, &params.tpu_addr) {
        (Some(identity), Some(tpu_addr)) => (
            Some(read_keypair_file(identity).unwrap()),
            Some(tpu_addr.parse().unwrap()),
        ),
        _ => (None, None),
    };

    let _metrics = metrics::spawn_metrics(params.metrics_addr.parse().unwrap());

    let tx_transactions = spawn_forwarder(
        identity,
        tpu_addr,
        params.rpc_url,
        params.blackhole,
        params.throttle_parallel,
    );

    let tasks: Vec<_> = params
        .grpc_urls
        .clone()
        .iter_mut()
        .map(|grpc_url| {
            let grpc_parsed_url: Uri = grpc_url.parse().unwrap();
            let mut feeder =
                metrics::spawn_feeder(grpc_parsed_url.host().unwrap_or("unknown").to_string());

            let tls_grpc_ca_cert = params.tls_grpc_ca_cert.clone();
            let tls_grpc_client_key = params.tls_grpc_client_key.clone();
            let tls_grpc_client_cert = params.tls_grpc_client_cert.clone();
            let tx_transactions = tx_transactions.clone();

            tokio::spawn(async move {
                loop {
                    if let Err(error) = spawn_grpc_client(
                        grpc_parsed_url.clone(),
                        tls_grpc_ca_cert.clone(),
                        tls_grpc_client_key.clone(),
                        tls_grpc_client_cert.clone(),
                        tx_transactions.clone(),
                        &mut feeder,
                    )
                    .await
                    {
                        error!("gRPC client failed: {error}");
                    }
                    sleep(Duration::from_millis(1_000)).await;
                }
            })
        })
        .collect();

    futures::future::join_all(tasks).await;

    info!("Service stopped.");
    Ok(())
}
