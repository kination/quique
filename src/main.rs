mod protocol;
mod server;
mod cluster;
mod queue;
mod storage;

use clap::Parser;
use tracing_subscriber::{FmtSubscriber, EnvFilter};
use server::Server;
use cluster::Cluster;

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
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env().add_directive("qbus=info".parse()?))
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let args = Args::parse();
    let cluster = Cluster::from_env()?;
    let srv = Server::new(args.addr, args.data_dir, cluster);
    /// println!("Server running in {:?}", args.addr);
    srv.run().await
}
