use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::protocol::*;

pub async fn connect(addr: &str) -> anyhow::Result<TcpStream> {
    Ok(TcpStream::connect(addr).await?)
}

pub async fn rpc(s: &mut TcpStream, op: Op, body: &BytesMut) -> anyhow::Result<(Status, Vec<u8>)> {
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

pub async fn redirecting_call_resp<F>(server: &str, op: Op, f: F) -> anyhow::Result<(Status, Vec<u8>)>
where
    F: Fn(&mut BytesMut) + Copy,
{
    let mut current = server.to_string();
    tracing::debug!("Current {:?}", current);
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
