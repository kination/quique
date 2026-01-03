use anyhow::Result;
use bytes::{BufMut, BytesMut};
// use std::sync::Arc;

use crate::cluster::Cluster;
use crate::protocol::*;
use crate::queue::Registry;

pub async fn handle_metadata(body: &mut &[u8], cluster: &Cluster, out: &mut BytesMut) -> Result<()> {
    // req: topic(str)
    let Some(topic) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    put_status(out, Status::Ok);
    // resp: [u32 1] then {u32 0 | str leader_addr}
    put_u32(out, 1);

    let leader = cluster.leader_of(&topic);
    out.put_u32(0);
    put_str(out, &leader.addr);
    Ok(())
}

pub async fn handle_create_topic(
    body: &mut &[u8],
    cluster: &Cluster,
    registry: &Registry,
    out: &mut BytesMut,
) -> Result<()> {
    // req: topic(str)
    let Some(topic) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };

    let leader = cluster.leader_of(&topic);
    if leader.id != cluster.me.id {
        put_status(out, Status::Redirect);
        put_str(out, &leader.addr);
        return Ok(());
    }

    if registry.get_topic(&topic).is_some() {
        put_status(out, Status::TopicExists);
        return Ok(());
    }

    let t = registry.create_topic(topic.clone());
    // Auto-create default queue with same name and bind
    registry.create_queue(topic.clone(), 1024);
    t.bind(topic.clone());

    put_status(out, Status::Ok);
    Ok(())
}

pub async fn handle_create_queue(
    body: &mut &[u8],
    registry: &Registry,
    out: &mut BytesMut,
) -> Result<()> {
    // req: queue(str) | capacity(u32)
    // Node-local operation. No redirection.
    let Some(queue) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let Some(cap) = get_u32(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };

    if registry.get_queue(&queue).is_some() {
        put_status(out, Status::TopicExists); // ResourceExists
        return Ok(());
    }

    registry.create_queue(queue, cap as usize);
    put_status(out, Status::Ok);
    Ok(())
}

pub async fn handle_bind_queue(
    body: &mut &[u8],
    cluster: &Cluster,
    registry: &Registry,
    out: &mut BytesMut,
) -> Result<()> {
    // req: topic(str) | queue(str)
    let Some(topic) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let Some(queue) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };

    let leader = cluster.leader_of(&topic);
    if leader.id != cluster.me.id {
        put_status(out, Status::Redirect);
        put_str(out, &leader.addr);
        return Ok(());
    }

    let Some(t) = registry.get_topic(&topic) else {
        put_status(out, Status::NotFound);
        return Ok(());
    };
    
    // Check if queue exists locally?
    if registry.get_queue(&queue).is_none() {
        put_status(out, Status::NotFound);
        return Ok(());
    }

    t.bind(queue);
    put_status(out, Status::Ok);
    Ok(())
}

pub async fn handle_produce(
    body: &mut &[u8],
    cluster: &Cluster,
    registry: &Registry,
    out: &mut BytesMut,
) -> Result<()> {
    // req : topic(str) | bytes
    let Some(topic) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let Some(data) = get_bytes(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };

    // Auto-create topic if not exists
    // We can just use create_topic which is idempotent now (returns existing or new)
    // But we need to check leadership first.
    let leader = cluster.leader_of(&topic);
    if leader.id != cluster.me.id {
        put_status(out, Status::Redirect);
        put_str(out, &leader.addr);
        return Ok(());
    }

    let t = registry.create_topic(topic.clone());
    
    // Auto-create default queue with same name and bind (Idempotent)
    registry.create_queue(topic.clone(), 1024);
    t.bind(topic.clone());

    // Fanout to all bound queues
    for q_name in t.bound_queues.iter() {
        if let Some(q) = registry.get_queue(&*q_name) {
            let _ = q.push(data.clone()); 
        }
    }

    put_status(out, Status::Ok);
    Ok(())
}

pub async fn handle_consume(
    body: &mut &[u8],
    _cluster: &Cluster,
    registry: &Registry,
    out: &mut BytesMut,
) -> Result<()> {
    // req : queue(str) | timeout_ms(u32, optional)
    let Some(queue_name) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let timeout = get_u32(body).unwrap_or(0);

    // Auto-create queue if not exists
    // Default capacity 1024?
    let q = registry.create_queue(queue_name.clone(), 1024);

    // Blocking consume
    // If timeout == 0, maybe non-blocking? Or infinite?
    // Redis BLPOP 0 means infinite.
    // Let's assume:
    // timeout == 0 => Non-blocking (return Empty if empty) - Wait, Redis BLPOP 0 IS infinite.
    // But standard Redis LPOP is non-blocking.
    // Our protocol had "timeout_ms" optional.
    // If client sends 0 or nothing, let's assume non-blocking for backward compat with our previous "Consume" logic?
    // But user asked for "Blocking Consume".
    // Let's define: if timeout > 0 => wait up to timeout.
    // If timeout == 0 => non-blocking (immediate).
    // If we want infinite blocking, we need a flag or special value.
    // Let's stick to: timeout=0 means non-blocking (legacy behavior).
    // We need a way to specify infinite blocking? Or just large timeout.
    // Let's change semantic: timeout=0 -> non-blocking. timeout>0 -> blocking ms.
    
    if timeout == 0 {
        match q.pop() {
            Some(v) => {
                put_status(out, Status::Ok);
                put_bytes(out, &v);
            }
            None => put_status(out, Status::Empty),
        }
    } else {
        // Blocking with timeout
        // We need `tokio::time::timeout`
        match tokio::time::timeout(std::time::Duration::from_millis(timeout as u64), q.pop_wait()).await {
            Ok(v) => {
                put_status(out, Status::Ok);
                put_bytes(out, &v);
            }
            Err(_) => {
                // Timeout expired
                put_status(out, Status::Empty);
            }
        }
    }
    Ok(())
}

pub async fn handle_read(
    _body: &mut &[u8],
    _cluster: &Cluster,
    _registry: &Registry,
    out: &mut BytesMut,
) -> Result<()> {
    // Read is for debugging WAL, but we removed WAL.
    // So just return Empty or BadRequest.
    put_status(out, Status::BadRequest);
    Ok(())
}