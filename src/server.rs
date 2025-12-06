use anyhow::Result;
use bytes::BytesMut;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{info, warn};
 
use crate::cluster::Cluster;
use crate::protocol::*;
use crate::queue::Registry;
 
use crate::handler;
 
pub struct Server {
    addr: String,
    data_dir: String,
    cluster: Cluster,
    registry: Arc<Registry>,
}

/// Central server application for messaging
impl Server {
    pub fn new(addr: String, data_dir: String, cluster: Cluster) -> Self {
        Self {
            addr,
            data_dir,
            cluster,
            registry: Arc::new(Registry::new()),
        }
    } 

    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        info!("quique server listening on {}", self.addr);

        loop {
            let (sock, _) = listener.accept().await?;
            let me = self.cluster.clone();
            let registry = self.registry.clone();
            let data_dir = self.data_dir.clone();
            tokio::spawn(async move {
                // info!("New connection on {:?}", sock.peer_addr());
                if let Err(e) = handle_conn(sock, me, registry, data_dir).await {
                    warn!("conn closed: {}", e);
                }
            });
        }
    }
}

async fn handle_conn(
    mut sock: TcpStream,
    cluster: Cluster,
    registry: Arc<Registry>,
    data_dir: String,
) -> Result<()> {

    // initialize memory space: 64kb
    // make space for memory buffer, to avoid assigining additional memory too often
    // TODO: setup value as config
    // - Make it bigger to avoid frequent memory assigning
    // - Make it smaller to avoid waste of memory if data traffic is small
    let mut buf = BytesMut::with_capacity(64 * 1024);

    loop {
        // assign additional memory if buffer is <1kb
        // TODO: setup value as config
        buf.reserve(1024);
        let n = sock.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        let hdr = match Header::decode(&mut buf)? {
            Some(h) => h,
            None => continue,  // if header is not fully arrived...
        };
        if buf.len() < hdr.body_len as usize {
            // keep going loop if body is not fully arrived
            continue;
        }
        let body = buf.split_to(hdr.body_len as usize).freeze();
        let mut body_slice = &body[..];

        let mut out = BytesMut::with_capacity(1024);
        let mut rh = Header {
            magic: 0,
            version: 0,
            op: hdr.op,
            flags: 0,
            stream_id: hdr.stream_id,
            body_len: 0,
        };

        match hdr.op {
            Op::Metadata => handler::handle_metadata(&mut body_slice, &cluster, &mut out).await?,
            Op::CreateTopic => handler::handle_create_topic(&mut body_slice, &cluster, &registry, &mut out).await?,
            Op::CreateQueue => handler::handle_create_queue(&mut body_slice, &registry, &mut out).await?,
            Op::BindQueue => handler::handle_bind_queue(&mut body_slice, &cluster, &registry, &mut out).await?,
            Op::Produce => handler::handle_produce(&mut body_slice, &cluster, &registry, &mut out).await?,
            Op::Consume => handler::handle_consume(&mut body_slice, &cluster, &registry, &mut out).await?,
            Op::Read => handler::handle_read(&mut body_slice, &cluster, &registry, &mut out).await?,
        }
 
        rh.body_len = out.len() as u32;
        rh.magic = MAGIC;
        rh.version = VERSION;
        let mut hb = BytesMut::with_capacity(16);
        rh.encode(&mut hb);
        sock.write_all(&hb).await?;
        sock.write_all(&out).await?;
    }
}

async fn write_err(sock: &mut TcpStream, mut rh: Header, st: Status) -> Result<()> {
    let mut out = BytesMut::new();
    put_status(&mut out, st);
    rh.body_len = out.len() as u32;
    rh.magic = MAGIC;
    rh.version = VERSION;
    let mut hdr = BytesMut::with_capacity(16);
    rh.encode(&mut hdr);
    sock.write_all(&hdr).await?;
    sock.write_all(&out).await?;
    Ok(())
}
