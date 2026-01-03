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

/// Messaging server application for messaging
impl Server {
    pub fn new(addr: String, data_dir: String, cluster: Cluster) -> Self {
        let registry = Arc::new(Registry::new(data_dir.clone()));
        
        // Load existing state
        if let Err(e) = registry.load() {
            tracing::warn!("Failed to load state, starting fresh: {}", e);
        }
        
        Self {
            addr,
            data_dir,
            cluster,
            registry,
        }
    }

    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        info!("toqueue server listening on {}", self.addr);
        let cluster = self.cluster;
        let registry = self.registry;
        let data_dir = self.data_dir;

        // Setup graceful shutdown
        let registry_for_shutdown = registry.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
            let mut sigint = signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");

            tokio::select! {
                _ = sigterm.recv() => info!("Received SIGTERM"),
                _ = sigint.recv() => info!("Received SIGINT (Ctrl+C)"),
            }

            info!("Shutting down, saving state...");
            if let Err(e) = registry_for_shutdown.save() {
                warn!("Failed to save state: {}", e);
            }
            std::process::exit(0);
        });

        loop {
            let (sock, _) = listener.accept().await?;
            let me = cluster.clone();
            let reg = registry.clone();
            let dir = data_dir.clone();
            
            tokio::spawn(async move {
                if let Err(e) = handle_conn(sock, me, reg, dir).await {
                    warn!("Connection closed: {}", e);
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

        // Read socket data, and write to buffer (non-blocking async read)
        let n = sock.read_buf(&mut buf).await?;
        if n == 0 {
            // connection closed, exit loop for graceful shutdown
            return Ok(());
        }

        // Try to decode 16-byte protocol header from buffer
        // Returns None if header not fully arrived yet (< 16 bytes)
        let hdr = match Header::decode(&mut buf)? {
            Some(h) => h,
            None => continue,  // Wait for more data
        };

        // Check if full message body has arrived
        // Header contains body_len field indicating expected payload size
        if buf.len() < hdr.body_len as usize {
            continue; // Wait for complete body
        }

        // Extract body from buffer and freeze it (convert to immutable Bytes)
        // split_to() removes first N bytes from buf and returns them
        let body = buf.split_to(hdr.body_len as usize).freeze();
        let mut body_slice = &body[..];

        // Prepare response buffer (1KB initial capacity)
        let mut out = BytesMut::with_capacity(1024);

        // Create response header, preserving op and stream_id from request
        // magic, version, body_len will be set after handler execution
        let mut rh = Header {
            magic: 0,
            version: 0,
            op: hdr.op,
            flags: 0,
            stream_id: hdr.stream_id,
            body_len: 0,
        };

        // Dispatch to appropriate handler based on operation type
        // Each handler reads from body_slice and writes response to out
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
