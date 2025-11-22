use anyhow::Result;
use bytes::{BufMut, BytesMut};
use std::sync::Arc;

use crate::cluster::Cluster;
use crate::protocol::*;
use crate::queue::{Topic, TopicRegistry};

pub async fn handle_metadata(body: &mut &[u8], cluster: &Cluster, out: &mut BytesMut) -> Result<()> {
    // req: topic(str)
    let Some(topic) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    put_status(out, Status::Ok);
    // resp: [u32 1] then {u32 0 | str leader_addr}
    // We pretend there is 1 partition (0) for compatibility if needed, or just simplify protocol.
    // Let's simplify: just return 1 partition always.
    put_u32(out, 1);

    let leader = cluster.leader_of(&topic);
    out.put_u32(0);
    put_str(out, &leader.addr);
    Ok(())
}

pub async fn handle_create_topic(
    body: &mut &[u8],
    cluster: &Cluster,
    topics: &TopicRegistry,
    data_dir: &str,
    out: &mut BytesMut,
) -> Result<()> {
    // req: topic(str) | capacity(u32)
    let Some(topic) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let Some(cap) = get_u32(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };

    if topics.get(&topic).is_some() {
        put_status(out, Status::TopicExists);
        return Ok(());
    }

    match Topic::open(data_dir, &topic, cap as usize, || {
            cluster.is_leader(&topic)
    }) {
        Ok(t) => {
        topics.insert(Arc::new(t));
        put_status(out, Status::Ok);
    }
        Err(_) => {
            // Not a leader for this topic, but another node might be.
            // For simplicity, we just say OK, assuming the client will get redirected
            // on produce/consume if this node isn't the leader.
            put_status(out, Status::Ok);
        }
    }
    Ok(())
}

pub async fn handle_produce(
    body: &mut &[u8],
    cluster: &Cluster,
    topics: &TopicRegistry,
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

    let Some(t) = topics.get(&topic) else {
        put_status(out, Status::NotFound);
        return Ok(());
    };
    let leader = cluster.leader_of(&topic);
    if leader.id != cluster.me.id {
        put_status(out, Status::Redirect);
        put_str(out, &leader.addr);
    } else {
        match t.enqueue(data) {
            Ok(_seq) => put_status(out, Status::Ok),
            Err(_) => put_status(out, Status::ServerError),
        }
    }
    Ok(())
}

pub async fn handle_consume(
    body: &mut &[u8],
    cluster: &Cluster,
    topics: &TopicRegistry,
    out: &mut BytesMut,
) -> Result<()> {
    // req : topic(str) | timeout_ms(u32, optional)
    let Some(topic) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let _timeout = get_u32(body).unwrap_or(0);

    let Some(t) = topics.get(&topic) else {
        put_status(out, Status::NotFound);
        return Ok(());
    };
    let leader = cluster.leader_of(&topic);
    if leader.id != cluster.me.id {
        put_status(out, Status::Redirect);
        put_str(out, &leader.addr);
    } else {
        match t.dequeue() {
            Ok(Some(v)) => {
                put_status(out, Status::Ok);
                put_bytes(out, &v);
            }
            Ok(None) => put_status(out, Status::Empty),
            Err(_) => put_status(out, Status::ServerError),
        }
    }
    Ok(())
}

pub async fn handle_read(body: &mut &[u8], cluster: &Cluster, topics: &TopicRegistry, out: &mut BytesMut) -> Result<()> {
    // req : topic(str) | size(u32)
    let (Some(topic), Some(size)) = (get_str(body), get_u32(body)) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };

    let Some(t) = topics.get(&topic) else {
        put_status(out, Status::NotFound);
        return Ok(());
    };
    let leader = cluster.leader_of(&topic);
    if leader.id != cluster.me.id {
        put_status(out, Status::Redirect);
        put_str(out, &leader.addr);
    } else {
        let messages = t.read_last_n(size as usize).unwrap_or_default();
        put_status(out, Status::Ok);
        put_u32(out, messages.len() as u32);
        for msg in messages {
            put_bytes(out, &msg);
        }
    }
    Ok(())
}