mod cluster;
mod handler;
mod protocol;
mod queue;
mod server;
mod storage;

use clap::Parser;
use cluster::Cluster;
use server::Server;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Parser, Debug)]
struct Args {
    /// listen addr
    #[arg(long, default_value = "127.0.0.1:7001")]
    addr: String,
    /// data dir
    #[arg(long, default_value = "./data")]
    data_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Define tracing subscriber for structured logging
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env().add_directive("quique=info".parse()?))
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let args = Args::parse();
    let cluster = Cluster::from_env()?;

    // start host server
    if let Err(e) = Server::new(args.addr, args.data_dir, cluster).run().await {
        tracing::error!("server error: {}", e);
        return Err(e);
    }

    Ok(())
}
