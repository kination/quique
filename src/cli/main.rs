use bytes::BytesMut;
use clap::{Parser, Subcommand};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use quique::client::*;
use quique::protocol::*;

#[derive(Parser, Debug)]
#[command(name = "qq-cli")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:7001")]
    server: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Create new topic
    CreateTopic {
        #[arg(long)]
        topic: String,
    },

    /// Create new queue
    CreateQueue {
        #[arg(long)]
        queue: String,
        #[arg(long, default_value_t = 1024)]
        capacity: u32,
    },

    /// Bind queue to topic
    BindQueue {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        queue: String,
    },

    /// Send value
    Produce {
        #[arg(long)]
        topic: String,

        #[arg(long)]
        data: String,
    },

    /// Fetch from queue
    Consume {
        #[arg(long)]
        queue: String,

        #[arg(long, default_value_t = 0)]
        timeout: u32,
    },
    /// Metadata dump
    Metadata {
        #[arg(long)]
        topic: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("error"))
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber).ok();

    let cli = Cli::parse();
    handle_command(cli.cmd, &cli.server).await
}

async fn handle_command(cmd: Cmd, server: &str) -> anyhow::Result<()> {
    match cmd {
        Cmd::CreateTopic { topic } => {
            tracing::debug!("Create topic {:?}", topic);
            call(&server, Op::CreateTopic, |b| {
                put_str(b, &topic);
            })
            .await?;
        }
        Cmd::CreateQueue { queue, capacity } => {
            tracing::debug!("Create queue {:?} {:?}", queue, capacity);
            call(&server, Op::CreateQueue, |b| {
                put_str(b, &queue);
                put_u32(b, capacity);
            })
            .await?;
        }
        Cmd::BindQueue { topic, queue } => {
            tracing::debug!("Bind queue {:?} to topic {:?}", queue, topic);
            call(&server, Op::BindQueue, |b| {
                put_str(b, &topic);
                put_str(b, &queue);
            })
            .await?;
        }
        Cmd::Produce { topic, data } => {
            let data_bytes = data.as_bytes();
            let (st, _payload) = redirecting_call_resp(&server, Op::Produce, |b| {
                put_str(b, &topic);
                put_bytes(b, data_bytes);
            })
            .await?;
            println!("status={:?}", st);
        }
        Cmd::Consume { queue, timeout } => {
            let (st, payload) = redirecting_call_resp(&server, Op::Consume, |b| {
                put_str(b, &queue);
                put_u32(b, timeout);
            })
            .await?;
            println!("status={:?}", st);
            if st == Status::Ok {
                if payload.len() >= 4 {
                    let n = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
                        as usize;
                    let v = &payload[4..4 + n];
                    println!("value={}", String::from_utf8_lossy(v));
                }
            }
        }
        Cmd::Metadata { topic } => {
            let mut s = connect(&server).await?;
            let mut body = BytesMut::new();
            put_str(&mut body, &topic);
            let (st, payload) = rpc(&mut s, Op::Metadata, &body).await?;
            println!("status={:?}", st);
            if st == Status::Ok {
                let mut b = &payload[..];
                if let Some(n) = get_u32(&mut b) {
                    for _ in 0..n {
                        let p = get_u32(&mut b).unwrap();
                        let addr = get_str(&mut b).unwrap();
                        println!("partition {} -> {}", p, addr);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn call<F>(server: &str, op: Op, f: F) -> anyhow::Result<()>
where
    F: Fn(&mut BytesMut) + Copy,
{
    let (st, _payload) = redirecting_call_resp(server, op, f).await?;
    println!("status={:?}", st);
    Ok(())
}
