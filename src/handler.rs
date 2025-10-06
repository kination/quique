use anyhow::Result;
use bytes::{BufMut, BytesMut};
use std::sync::Arc;

use crate::cluster::Cluster;
use crate::protocol::*;
use crate::queue::{Topic, TopicRegistry};

pub async fn handle_metadata(body: &mut &[u8], cluster: &Cluster, out: &mut BytesMut) -> Result<()> {
    // req: topic(str) | partitions(u32)
    let Some(topic) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let Some(parts) = get_u32(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    put_status(out, Status::Ok);
    // resp: [u32 N] then N x {u32 part | str leader_addr}
    put_u32(out, parts);

    for p in 0..parts {
        let leader = cluster.leader_of(&topic, p);
        out.put_u32(p);
        put_str(out, &leader.addr);
    }
    Ok(())
}

pub async fn handle_create_topic(
    body: &mut &[u8],
    cluster: &Cluster,
    topics: &TopicRegistry,
    data_dir: &str,
    out: &mut BytesMut,
) -> Result<()> {
    // req: topic(str) | partitions(u32) | capacity(u32)
    let Some(topic) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let Some(parts) = get_u32(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let Some(cap) = get_u32(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };

    if topics.get(&topic).is_some() {
        put_status(out, Status::TopicExists);
    } else {
        let t = Topic::open(data_dir, &topic, parts, cap as usize, |p| {
            cluster.is_leader(&topic, p)
        })?;
        topics.insert(Arc::new(t));
        put_status(out, Status::Ok);
    }
    Ok(())
}

pub async fn handle_produce(
    body: &mut &[u8],
    cluster: &Cluster,
    topics: &TopicRegistry,
    out: &mut BytesMut,
) -> Result<()> {
    // req : topic(str) | key(str) | bytes
    let Some(topic) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let Some(key) = get_str(body) else {
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
    let pidx = t.pick_partition_by_key(&key) as u32;
    let leader = cluster.leader_of(&topic, pidx);
    if leader.id != cluster.me.id {
        put_status(out, Status::Redirect);
        put_str(out, &leader.addr);
    } else {
        let part = &t.partitions[pidx as usize];
        match part.enqueue(data) {
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
    // req : topic(str) | key(str) | timeout_ms(u32, optional)
    let Some(topic) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let Some(key) = get_str(body) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };
    let _timeout = get_u32(body).unwrap_or(0);

    let Some(t) = topics.get(&topic) else {
        put_status(out, Status::NotFound);
        return Ok(());
    };
    let pidx = t.pick_partition_by_key(&key) as u32;
    let leader = cluster.leader_of(&topic, pidx);
    if leader.id != cluster.me.id {
        put_status(out, Status::Redirect);
        put_str(out, &leader.addr);
    } else {
        let part = &t.partitions[pidx as usize];
        match part.dequeue() {
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
    // req : topic(str) | partition(u32) | size(u32)
    let (Some(topic), Some(pidx), Some(size)) = (get_str(body), get_u32(body), get_u32(body)) else {
        put_status(out, Status::BadRequest);
        return Ok(());
    };

    let Some(t) = topics.get(&topic) else {
        put_status(out, Status::NotFound);
        return Ok(());
    };
    let leader = cluster.leader_of(&topic, pidx);
    if leader.id != cluster.me.id {
        put_status(out, Status::Redirect);
        put_str(out, &leader.addr);
    } else {
        let part = &t.partitions[pidx as usize];
        let messages = part.read_last_n(size as usize).unwrap_or_default();
        put_status(out, Status::Ok);
        put_u32(out, messages.len() as u32);
        for msg in messages {
            put_bytes(out, &msg);
        }
    }
    Ok(())
}