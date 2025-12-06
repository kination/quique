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

    registry.create_topic(topic);
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
    // If we bind a queue that doesn't exist locally, produce will fail to push.
    // But maybe we allow binding non-existent queues (they might be created later).
    // But for safety, let's check.
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

    let Some(t) = registry.get_topic(&topic) else {
        put_status(out, Status::NotFound);
        return Ok(());
    };

    // Check leadership?
    // User said "Retention ... later", "Cluster ... later".
    // But existing code checks leadership.
    // If we want to keep it simple, we can ignore leadership for now or keep it.
    // The user said "Cluster ... later", so maybe single node for now?
    // But `cluster` arg is still here.
    // Let's keep leadership check for Topic to be safe, or remove it if we want to be purely local.
    // "Producer sends to topic ... then sends to queues".
    // If queues are on different nodes?
    // User said "internal memory based queue".
    // Let's assume single node or simple cluster where topic leader handles it.
    let leader = cluster.leader_of(&topic);
    if leader.id != cluster.me.id {
        put_status(out, Status::Redirect);
        put_str(out, &leader.addr);
        return Ok(());
    }

    // Fanout to all bound queues
    // We need to look up queues by name.
    // If a queue is not found (deleted?), we skip it or error?
    // RabbitMQ drops if no route, or returns unroutable.
    // Here we just try to push to all bound queues.
    for q_name in t.bound_queues.iter() {
        if let Some(q) = registry.get_queue(&*q_name) {
            let _ = q.push(data.clone()); // Ignore full queues? or error?
            // "Internal memory based queue" -> if full, maybe drop or block?
            // ArrayQueue returns Err if full.
            // We just ignore errors for now to avoid blocking the whole produce.
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
    // Note: Protocol says "topic" in previous version, but we interpret it as queue name now.
    let Some(queue_name) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let _timeout = get_u32(body).unwrap_or(0);

    let Some(q) = registry.get_queue(&queue_name) else {
        put_status(out, Status::NotFound);
        return Ok(());
    };

    // No leadership check for Queue?
    // If queues are local memory, we must be on the node that holds the queue.
    // But `Registry` is local.
    // If we are in a cluster, we need to know where the queue lives.
    // For now, assume all queues are local or replicated?
    // "internal memory based queue" -> Local.
    // If we are not the leader for the *Topic*, we might still hold the *Queue*?
    // User said "Producer sends to topic ... then sends to queues".
    // This implies Topic and Queue might be on same node or different.
    // Given "internal memory", let's assume everything is local for this refactor step.

    match q.pop() {
        Some(v) => {
            put_status(out, Status::Ok);
            put_bytes(out, &v);
        }
        None => put_status(out, Status::Empty),
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