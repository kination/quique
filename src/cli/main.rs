use bytes::BytesMut;
use clap::{Parser, Subcommand};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
    Create {
        #[arg(long)]
        topic: String,

        #[arg(long, default_value_t = 1024)]
        capacity: u32,
    },

    /// Send value
    Produce {
        #[arg(long)]
        topic: String,

        #[arg(long)]
        data: String,
    },

    /// Fetch from topic
    Consume {
        #[arg(long)]
        topic: String,
    },
    /// Metadata dump
    Metadata {
        #[arg(long)]
        topic: String,
    },

    /// Read last N messages from a topic (for debugging)
    Read {
        #[arg(long)]
        topic: String,

        #[arg(long, default_value_t = 10)]
        size: u32,
    },
}

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Ok = 0,
    Redirect = 10,
    Empty = 11,
    TopicExists = 12,
    NotFound = 13,
    BadRequest = 400,
    ServerError = 500,
}

// a tiny From for printing convenience
impl From<u16> for Status {
    fn from(v: u16) -> Self {
        match v {
            0 => Status::Ok,
            10 => Status::Redirect,
            11 => Status::Empty,
            12 => Status::TopicExists,
            13 => Status::NotFound,
            400 => Status::BadRequest,
            _ => Status::ServerError,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    handle_command(cli.cmd, &cli.server).await
}

async fn handle_command(cmd: Cmd, server: &str) -> anyhow::Result<()> {
    match cmd {
        Cmd::Create {
            topic,
            capacity,
        } => {
            println!("Create topic {:?} {:?}", topic, capacity);
            call(&server, Op::CreateTopic, |b| {
                put_str(b, &topic);
                put_u32(b, capacity);
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
        Cmd::Consume { topic } => {
            let (st, payload) = redirecting_call_resp(&server, Op::Consume, |b| {
                put_str(b, &topic);
                put_u32(b, 0);
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
        Cmd::Read {
            topic,
            size,
        } => {
            let (st, payload) = redirecting_call_resp(&server, Op::Read, |b| {
                put_str(b, &topic);
                put_u32(b, size);
            })
            .await?;
            println!("status={:?}", st);
            if st == Status::Ok {
                let mut b = &payload[..];
                if let Some(n) = get_u32(&mut b) {
                    if n == 0 {
                        println!(
                            "No messages found in topic '{}'.",
                            topic
                        );
                        return Ok(());
                    }
                    println!(
                        "Found {} messages in topic '{}':",
                        n, topic
                    );
                    for i in 0..n {
                        if let Some(msg) = get_bytes(&mut b) {
                            println!("[{}] {}", i, String::from_utf8_lossy(&msg));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

async fn connect(addr: &str) -> anyhow::Result<TcpStream> {
    Ok(TcpStream::connect(addr).await?)
}

async fn call<F>(server: &str, op: Op, f: F) -> anyhow::Result<()>
where
    F: Fn(&mut BytesMut) + Copy,
{
    let (st, _payload) = redirecting_call_resp(server, op, f).await?;
    println!("status={:?}", st);
    Ok(())
}

async fn redirecting_call_resp<F>(server: &str, op: Op, f: F) -> anyhow::Result<(Status, Vec<u8>)>
where
    F: Fn(&mut BytesMut) + Copy,
{
    let mut current = server.to_string();
    println!("Current {:?}", current);
    for _ in 0..5 {
        let mut s = connect(&current).await?;
        let mut body = BytesMut::new();
        f(&mut body);
        let (st, payload) = rpc(&mut s, op, &body).await?;
        if st == Status::Redirect {
            let mut b = &payload[..];
            let addr = get_str(&mut b).unwrap();
            current = addr;
            continue;
        }
        return Ok((st, payload));
    }
    anyhow::bail!("too many redirects")
}

async fn rpc(s: &mut TcpStream, op: Op, body: &BytesMut) -> anyhow::Result<(Status, Vec<u8>)> {
    let hdr = Header {
        magic: MAGIC,
        version: VERSION,
        op,
        flags: 0,
        stream_id: 0,
        body_len: body.len() as u32,
    };
    let mut buf = BytesMut::with_capacity(16 + body.len());
    hdr.encode(&mut buf);
    buf.extend_from_slice(&body);
    s.write_all(&buf).await?;

    let mut hb = [0u8; 16];
    s.read_exact(&mut hb).await?;
    let body_len = u32::from_be_bytes([hb[12], hb[13], hb[14], hb[15]]) as usize;
    let mut body = vec![0u8; body_len];
    s.read_exact(&mut body).await?;
    let st = Status::from(u16::from_be_bytes([body[0], body[1]]));
    Ok((st, body[2..].to_vec()))
}
