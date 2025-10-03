use std::sync::Arc;
use bytes::{BufMut, BytesMut};
use tokio::{net::{TcpListener, TcpStream}, io::{AsyncReadExt, AsyncWriteExt}};
use tracing::{info, warn};
use anyhow::Result;

use crate::protocol::*;
use crate::queue::{TopicRegistry, Topic};
use crate::cluster::Cluster;

pub struct Server {
    addr: String,
    data_dir: String,
    cluster: Cluster,
    topics: Arc<TopicRegistry>,
}

impl Server {
    pub fn new(addr: String, data_dir: String, cluster: Cluster) -> Self {
        Self { addr, data_dir, cluster, topics: Arc::new(TopicRegistry::new()) }
    }

    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("qbus listening on {}", self.addr);
        
        loop {
            let (sock, _) = listener.accept().await?;
            let me = self.cluster.clone();
            let topics = self.topics.clone();
            let data_dir = self.data_dir.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_conn(sock, me, topics, data_dir).await {
                    warn!("conn closed: {}", e);
                }
            });
        }
    }
}

async fn handle_conn(mut sock: TcpStream, cluster: Cluster, topics: Arc<TopicRegistry>, data_dir: String) -> Result<()> {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
        buf.reserve(1024);
        let n = sock.read_buf(&mut buf).await?;
        if n == 0 { return Ok(()); }

        let hdr = match Header::decode(&mut buf)? { Some(h) => h, None => continue };
        if buf.len() < hdr.body_len as usize { continue; }
        let body = buf.split_to(hdr.body_len as usize).freeze();
        let mut body_slice = &body[..];

        let mut out = BytesMut::with_capacity(1024);
        let mut rh = Header { magic: 0, version: 0, op: hdr.op, flags: 0, stream_id: hdr.stream_id, body_len: 0 };

        match hdr.op {
            Op::Metadata => {
                // req: topic(str) | partitions(u32)
                let Some(topic) = get_str(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };
                let Some(parts) = get_u32(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };
                put_status(&mut out, Status::Ok);
                // 응답: [u32 N] then N x {u32 part | str leader_addr}
                put_u32(&mut out, parts);
                
                for p in 0..parts {
                    let leader = cluster.leader_of(&topic, p);
                    out.put_u32(p);
                    put_str(&mut out, &leader.addr);
                }
            }

            Op::CreateTopic => {
                println!("Handle CreateTopic : {:?}", body_slice);
                // topic(str) | partitions(u32) | capacity(u32)
                let Some(topic) = get_str(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };
                let Some(parts) = get_u32(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };
                let Some(cap)   = get_u32(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };
                if topics.get(&topic).is_some() {
                    println!("Handle topic {:?}", topic);
                    put_status(&mut out, Status::TopicExists);
                } else {
                    // 모든 노드는 Topic 객체를 메모리에 로드합니다.
                    // 단, 자신이 리더인 파티션에 대해서만 실제 파일/큐를 생성합니다.
                    let t = Topic::open(&data_dir, &topic, parts, cap as usize, |p| cluster.is_leader(&topic, p))?;
                    topics.insert(Arc::new(t));
                    put_status(&mut out, Status::Ok);
                }
            }

            Op::Produce => {
                // topic(str) | key(str) | bytes
                let Some(topic) = get_str(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };
                let Some(key)   = get_str(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };
                let Some(data)  = get_bytes(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };

                let Some(t) = topics.get(&topic) else { write_err(&mut sock, rh, Status::NotFound).await?; continue; };
                let pidx = t.pick_partition_by_key(&key) as u32;
                let leader = cluster.leader_of(&topic, pidx);
                if leader.id != cluster.me.id {
                    put_status(&mut out, Status::Redirect);
                    put_str(&mut out, &leader.addr);
                } else {
                    let part = &t.partitions[pidx as usize];
                    match part.enqueue(data) {
                        Ok(_seq) => { put_status(&mut out, Status::Ok); }
                        Err(_) =>  { put_status(&mut out, Status::ServerError); }
                    }
                }
            }

            Op::Consume => {
                // topic(str) | key(str) | timeout_ms(u32, optional)
                let Some(topic) = get_str(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };
                let Some(key)   = get_str(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };
                let _timeout    = get_u32(&mut body_slice).unwrap_or(0);

                let Some(t) = topics.get(&topic) else { write_err(&mut sock, rh, Status::NotFound).await?; continue; };
                let pidx = t.pick_partition_by_key(&key) as u32;
                let leader = cluster.leader_of(&topic, pidx);
                if leader.id != cluster.me.id {
                    put_status(&mut out, Status::Redirect);
                    put_str(&mut out, &leader.addr);
                } else {
                    let part = &t.partitions[pidx as usize];
                    match part.dequeue() {
                        Ok(Some(v)) => { put_status(&mut out, Status::Ok); put_bytes(&mut out, &v); }
                        Ok(None)    => { put_status(&mut out, Status::Empty); }
                        Err(_)      => { put_status(&mut out, Status::ServerError); }
                    }
                }
            }

            Op::Read => {
                // topic(str) | partition(u32) | size(u32)
                let Some(topic) = get_str(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };
                let Some(pidx) = get_u32(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };
                let Some(size) = get_u32(&mut body_slice) else { write_err(&mut sock, rh, Status::BadRequest).await?; continue; };

                let Some(t) = topics.get(&topic) else { write_err(&mut sock, rh, Status::NotFound).await?; continue; };
                let leader = cluster.leader_of(&topic, pidx);
                if leader.id != cluster.me.id {
                    put_status(&mut out, Status::Redirect);
                    put_str(&mut out, &leader.addr);
                } else {
                    let part = &t.partitions[pidx as usize];
                    let messages = part.read_last_n(size as usize).unwrap_or_default();
                    put_status(&mut out, Status::Ok);
                    put_u32(&mut out, messages.len() as u32);
                    for msg in messages {
                        put_bytes(&mut out, &msg);
                    }
                }
            }
        }

        rh.body_len = out.len() as u32; rh.magic = MAGIC; rh.version = VERSION;
        let mut hb = BytesMut::with_capacity(16); rh.encode(&mut hb);
        sock.write_all(&hb).await?; sock.write_all(&out).await?;
    }
}

async fn write_err(sock: &mut TcpStream, mut rh: Header, st: Status) -> Result<()> {
    let mut out = BytesMut::new(); put_status(&mut out, st);
    rh.body_len = out.len() as u32; rh.magic = MAGIC; rh.version = VERSION;
    let mut hdr = BytesMut::with_capacity(16); rh.encode(&mut hdr);
    sock.write_all(&hdr).await?; sock.write_all(&out).await?; Ok(())
}
